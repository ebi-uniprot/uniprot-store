package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.uniprot.common.PublicationDateFormatter;
import uk.ac.ebi.uniprot.cv.keyword.KeywordDetail;
import uk.ac.ebi.uniprot.cv.pathway.UniPathway;
import uk.ac.ebi.uniprot.domain.DBCrossReference;
import uk.ac.ebi.uniprot.domain.Property;
import uk.ac.ebi.uniprot.domain.Sequence;
import uk.ac.ebi.uniprot.domain.Value;
import uk.ac.ebi.uniprot.domain.citation.Citation;
import uk.ac.ebi.uniprot.domain.citation.CitationXrefType;
import uk.ac.ebi.uniprot.domain.citation.JournalArticle;
import uk.ac.ebi.uniprot.domain.gene.Gene;
import uk.ac.ebi.uniprot.domain.impl.SequenceImpl;
import uk.ac.ebi.uniprot.domain.uniprot.*;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtAccessionBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtEntryBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtIdBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.comment.*;
import uk.ac.ebi.uniprot.domain.uniprot.description.*;
import uk.ac.ebi.uniprot.domain.uniprot.evidence.Evidence;
import uk.ac.ebi.uniprot.domain.uniprot.feature.Feature;
import uk.ac.ebi.uniprot.domain.uniprot.taxonomy.OrganismHost;
import uk.ac.ebi.uniprot.domain.uniprot.xdb.UniProtDBCrossReference;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.FFLineBuilder;
import uk.ac.ebi.uniprot.flatfile.parser.impl.cc.CCLineBuilderFactory;
import uk.ac.ebi.uniprot.flatfile.parser.impl.ft.FeatureLineBuilderFactory;
import uk.ac.ebi.uniprot.flatfile.parser.impl.ra.RALineBuilder;
import uk.ac.ebi.uniprot.flatfile.parser.impl.rg.RGLineBuilder;
import uk.ac.ebi.uniprot.indexer.common.DocumentConversionException;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTerm;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomicNode;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.indexer.util.DateUtils;
import uk.ac.ebi.uniprot.json.parser.uniprot.UniprotJsonConfig;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;
import uk.ebi.uniprot.scorer.uniprotkb.UniProtEntryScored;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * // TODO: 18/04/19 can be moved to a different package?
 *
 * Created 18/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryConverter implements DocumentConverter<UniProtEntry, UniProtDocument> {
    static final int SORT_FIELD_MAX_LENGTH = 30;
    static final int MAX_STORED_FIELD_LENGTH = 32766;
    private static final Logger LOGGER = LoggerFactory.getLogger(UniProtEntryConverter.class);

    private static final String XREF = "xref_";
    private static final String COMMENT = "cc_";
    private static final String CC_EV = "ccev_";
    private static final String FEATURE = "ft_";
    private static final String FT_EV = "ftev_";
    private static final String FT_LENGTH = "ftlen_";
    private static final String DASH = "-";
    private static final String GO = "go_";
    private static final Map<Integer, String> POPULAR_ORGANIMS_TAX_NAME
            = Collections.unmodifiableMap(new HashMap<Integer, String>() {{
        put(9606, "Human");
        put(10090, "Mouse");
        put(10116, "Rat");
        put(9913, "Bovine");
        put(7955, "Zebrafish");
        put(7227, "Fruit fly");
        put(6239, "C. elegans");
        put(44689, "Slime mold");
        put(3702, "A. thaliana");
        put(39947, "Rice");
        put(83333, "E. coli K12");
        put(224308, "B. subtilis");
        put(559292, "S. cerevisiae");
    }});
    private static final Pattern PATTERN_FAMILY = Pattern
            .compile("(?:In the .+? section; )?[Bb]elongs to the (.+?family)\\.(?: (.+?family)\\.)?(?: (.+?family)\\.)?(?: Highly divergent\\.)?");
    /**
     * An enum set representing all of the organelles that are children of plastid
     */
    private static final EnumSet<GeneEncodingType> PLASTID_CHILD = EnumSet.of(GeneEncodingType.APICOPLAST_PLASTID,
                                                                              GeneEncodingType.CHLOROPLAST_PLASTID, GeneEncodingType.CYANELLE_PLASTID,
                                                                              GeneEncodingType.NON_PHOTOSYNTHETIC_PLASTID, GeneEncodingType.CHROMATOPHORE_PLASTID);
    private static final String MANUAL_EVIDENCE = "manual";
    private static final List<String> MANUAL_EVIDENCE_MAP = Arrays.asList("ECO_0000269", "ECO_0000303", "ECO_0000305",
                                                                          "ECO_0000250", "ECO_0000255", "ECO_0000244", "ECO_0000312");
    private static final String AUTOMATIC_EVIDENCE = "automatic";
    private static final List<String> AUTOMATIC_EVIDENCE_MAP = Arrays.asList("ECO_0000256", "ECO_0000213",
                                                                             "ECO_0000313", "ECO_0000259");
    private static final String EXPERIMENTAL_EVIDENCE = "experimental";
    private static final List<String> EXPERIMENTAL_EVIDENCE_MAP = Collections.singletonList("ECO_0000269");
    private final TaxonomyRepo taxonomyRepo;
    private final RALineBuilder raLineBuilder;
    private final RGLineBuilder rgLineBuilder;
    private final GoRelationRepo goRelationRepo;
    private final KeywordRepo keywordRepo;
    private final PathwayRepo pathwayRepo;
    //  private final UniProtUniRefMap uniprotUniRefMap;


    public UniProtEntryConverter(TaxonomyRepo taxonomyRepo, GoRelationRepo goRelationRepo,

                                 //	UniProtUniRefMap uniProtUniRefMap,
                                 KeywordRepo keywordRepo, PathwayRepo pathwayRepo
    ) {
        this.taxonomyRepo = taxonomyRepo;
        this.goRelationRepo = goRelationRepo;
        this.keywordRepo = keywordRepo;
        this.pathwayRepo = pathwayRepo;
        //   this.uniprotUniRefMap = uniProtUniRefMap;

        raLineBuilder = new RALineBuilder();
        rgLineBuilder = new RGLineBuilder();
    }

    @Override
    public UniProtDocument convert(UniProtEntry source) {
        try {
            UniProtDocument japiDocument = new UniProtDocument();
            // 1 UniProtEntry creates a single JAPIDocument

            japiDocument.accession = source.getPrimaryAccession().getValue();
            if (japiDocument.accession.contains(DASH)) {
                if (this.isCanonicalIsoform(source)) {
                    japiDocument.reviewed = null;
                    japiDocument.isIsoform = null;
                    return japiDocument;
                }
                japiDocument.isIsoform = true;
                // We are adding the canonical accession to the isoform entry as a secondary accession.
                // this way when you search by an accession and ask to include isoforms, it will find it.
                String canonicalAccession = japiDocument.accession.substring(0, japiDocument.accession.indexOf(DASH));
                japiDocument.secacc.add(canonicalAccession);
            } else {
                japiDocument.isIsoform = false;
            }
            japiDocument.id = source.getUniProtId().getValue();
            addValueListToStringList(japiDocument.secacc, source.getSecondaryAccessions());

            japiDocument.reviewed = (source.getEntryType() == UniProtEntryType.SWISSPROT);

            EntryAudit entryAudit = source.getEntryAudit();
            if (entryAudit != null) {
                japiDocument.firstCreated = DateUtils.convertLocalDateToDate(entryAudit.getFirstPublicDate());
                japiDocument.lastModified = DateUtils.convertLocalDateToDate(entryAudit.getLastAnnotationUpdateDate());
                japiDocument.sequenceUpdated = DateUtils.convertLocalDateToDate(entryAudit.getLastSequenceUpdateDate());
            }
            setProteinNames(source, japiDocument);
            setECNumbers(source, japiDocument);
            setKeywords(source,japiDocument);
            setOrganism(source, japiDocument);
            setLineageTaxons(source, japiDocument);
            setOrganismHosts(source, japiDocument);
            convertGeneNames(source, japiDocument);
            if (!japiDocument.geneNamesExact.isEmpty()) {
                japiDocument.geneNames.addAll(japiDocument.geneNamesExact);
                japiDocument.geneNamesSort = truncatedSortValue(String.join(" ",japiDocument.geneNames));
            }
            convertGoTerms(source, japiDocument);
            convertReferences(source, japiDocument);
            convertComment(source, japiDocument);
            convertXref(source, japiDocument);
            setOrganelle(source, japiDocument);
            convertFeature(source, japiDocument);
            setFragmentNPrecursor(source, japiDocument);
            setProteinExistence(source, japiDocument);
            setSequence(source, japiDocument);
            setScore(source, japiDocument);
            setAvroDefaultEntry(source, japiDocument);
            setDefaultSearchContent(japiDocument);
            setUniRefClusters(japiDocument.accession, japiDocument);
            return japiDocument;
        } catch (IllegalArgumentException | NullPointerException e) {
            String message = "Error converting UniProt entry: " + source.getPrimaryAccession().getValue();
            log.error(message, e);
            throw new DocumentConversionException(message, e);
        }
    }

    private void setDefaultSearchContent(UniProtDocument japiDocument) {
        japiDocument.content.add(japiDocument.accession);
        japiDocument.content.addAll(japiDocument.secacc);
        japiDocument.content.add(japiDocument.id); //mnemonic

        japiDocument.content.addAll(japiDocument.proteinNames);
        japiDocument.content.addAll(japiDocument.keywords);
        japiDocument.content.addAll(japiDocument.geneNames);

        japiDocument.content.add(String.valueOf(japiDocument.organismTaxId));
        japiDocument.content.addAll(japiDocument.organismName);
        japiDocument.content.addAll(japiDocument.organismHostNames);
        japiDocument.content.addAll(japiDocument.organismHostIds.stream().map(String::valueOf).collect(Collectors.toList()));

        japiDocument.content.addAll(japiDocument.organelles);

        japiDocument.content.addAll(japiDocument.referenceAuthors);
        japiDocument.content.addAll(japiDocument.referenceJournals);
        japiDocument.content.addAll(japiDocument.referenceOrganizations);
        japiDocument.content.addAll(japiDocument.referencePubmeds);
        japiDocument.content.addAll(japiDocument.referenceTitles);

        japiDocument.content.addAll(japiDocument.proteomes);
        japiDocument.content.addAll(japiDocument.proteomeComponents);

        japiDocument.content.addAll(japiDocument.xrefs);
        japiDocument.content.addAll(japiDocument.databases);

        // IMPORTANT: we also add information to content in other methods:
        //comments --> see convertComment method
        //features --> see convertFeature method
        //go Terms --> see convertGoTerms method
    }

    public boolean isCanonicalIsoform(UniProtEntry uniProtEntry) {
        return uniProtEntry.getCommentByType(CommentType.ALTERNATIVE_PRODUCTS)
                .stream()
                .map(comment -> (AlternativeProductsComment) comment)
                .flatMap(comment -> comment.getIsoforms().stream())
                .filter(isoform -> isoform.getIsoformSequenceStatus() == IsoformSequenceStatus.DISPLAYED)
                .flatMap(isoform -> isoform.getIsoformIds().stream())
                .filter(isoformId -> isoformId.getValue().equals(uniProtEntry.getPrimaryAccession().getValue()))
                .count() == 1L;
    }

    static String truncatedSortValue(String value) {
        if (value.length() > SORT_FIELD_MAX_LENGTH) {
            return value.substring(0, SORT_FIELD_MAX_LENGTH);
        } else {
            return value;
        }
    }

    private void setUniRefClusters(String accession, UniProtDocument japiDocument) {
        //     japiDocument.unirefCluster50 = uniprotUniRefMap.getMappings50(accession);
        //      japiDocument.unirefCluster90 = uniprotUniRefMap.getMappings90(accession);
        //      japiDocument.unirefCluster100 = uniprotUniRefMap.getMappings100(accession);
    }

    private static void addValueListToStringList(Collection<String> list, List<? extends Value> values) {
        if (values != null) {
            for (Value v : values) {
                addValueToStringList(list, v);
            }
        }
    }

    private static void addValueToStringList(Collection<String> list, Value value) {
        if ((value != null) && (!value.getValue().isEmpty())) {
            list.add(value.getValue());
        }
    }

    private void setAvroDefaultEntry(UniProtEntry source, UniProtDocument japiDocument) {
        try {
            UniProtAccession accession = new UniProtAccessionBuilder(source.getPrimaryAccession().getValue()).build();
            UniProtId uniProtId = new UniProtIdBuilder(source.getUniProtId().getValue()).build();
            UniProtEntry defaultObject = new UniProtEntryBuilder()
                    .primaryAccession(accession)
                    .uniProtId(uniProtId)
                    .active()
                    .entryType(UniProtEntryType.valueOf(source.getEntryType().name()))
                    .proteinDescription(source.getProteinDescription())
                    .genes(source.getGenes())
                    .organism(source.getOrganism())
                    .sequence(new SequenceImpl(source.getSequence().getValue()))
                    .build();

            byte[] avroByteArray = UniprotJsonConfig.getInstance().getDefaultFullObjectMapper().writeValueAsBytes(defaultObject);
            // can only store if it's size is small enough:
            // https://lucene.apache.org/core/7_5_0/core/org/apache/lucene/index/DocValuesType.html
            japiDocument.avro_binary = getDefaultBinaryValue(Base64.getEncoder().encodeToString(avroByteArray));
        }catch (JsonProcessingException exception){
            String accession = source.getPrimaryAccession().getValue();
            LOGGER.warn("Error saving default uniprot object in avro_binary for accession: "+accession,exception);
        }
    }

    static String getDefaultBinaryValue(String base64Value) {
        if (base64Value.length() <= MAX_STORED_FIELD_LENGTH) {
            return base64Value;
        } else {
            return null;
        }
    }

    private void setScore(UniProtEntry source, UniProtDocument japiDocument) {
        UniProtEntryScored entryScored = new UniProtEntryScored(source);
        double score = entryScored.score();
        int q = (int) (score / 20d);
        japiDocument.score = q > 4 ? 5 : q + 1;
    }

    private void setSequence(UniProtEntry source, UniProtDocument japiDocument) {
        Sequence seq = source.getSequence();
        japiDocument.seqLength = seq.getLength();
        japiDocument.seqMass = seq.getMolWeight();
    }

    private void setProteinNames(UniProtEntry source, UniProtDocument japiDocument) {
        if(source.hasProteinDescription()) {
            ProteinDescription proteinDescription = source.getProteinDescription();
            List<String> names = extractProteinDescriptionValues(proteinDescription);
            japiDocument.proteinNames.addAll(names);
            japiDocument.proteinsNamesSort = truncatedSortValue(String.join(" ", names));
        }
    }

    private void setECNumbers(UniProtEntry source, UniProtDocument japiDocument) {
        if(source.hasProteinDescription()) {
            ProteinDescription proteinDescription = source.getProteinDescription();
            japiDocument.ecNumbers = extractProteinDescriptionEcs(proteinDescription);
            japiDocument.ecNumbersExact = japiDocument.ecNumbers;
        }
    }

    private void setKeywords(UniProtEntry source,UniProtDocument japiDocument){
        if(source.hasKeywords()) {
            source.getKeywords().forEach(keyword -> {
                updateKeyword(keyword, japiDocument);
            });
        }
    }

    private void updateKeyword(Keyword keyword, UniProtDocument japiDocument) {
        japiDocument.keywordIds.add(keyword.getId());
        japiDocument.keywords.add(keyword.getId());
        addValueToStringList(japiDocument.keywords, keyword);
        Optional<KeywordDetail> kwOp = keywordRepo.getKeyword(keyword.getId());
        if(kwOp.isPresent()) {
            if((kwOp.get().getCategory() !=null) &&
                    !japiDocument.keywordIds.contains(kwOp.get().getCategory().getKeyword().getAccession())){
                japiDocument.keywordIds.add(kwOp.get().getCategory().getKeyword().getAccession());
                japiDocument.keywords.add(kwOp.get().getCategory().getKeyword().getAccession());
                japiDocument.keywords.add(kwOp.get().getCategory().getKeyword().getId());
            }
        }
    }

    private void setOrganism(UniProtEntry source, UniProtDocument japiDocument) {
        if (source.hasOrganism()) {
            int taxonomyId = Math.toIntExact(source.getOrganism().getTaxonId());
            japiDocument.organismTaxId = taxonomyId;

            Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
            if (taxonomicNode.isPresent()) {
                TaxonomicNode node = taxonomicNode.get();
                List<String> extractedTaxoNode = extractTaxonode(node);
                japiDocument.organismName.addAll(extractedTaxoNode);
                japiDocument.organismSort = truncatedSortValue(String.join(" ", extractedTaxoNode));

                String popularOrgamism = POPULAR_ORGANIMS_TAX_NAME.get(taxonomyId);
                if (popularOrgamism != null) {
                    japiDocument.popularOrganism = popularOrgamism;
                } else {
                    if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
                        japiDocument.otherOrganism = node.mnemonic();
                    } else if (node.commonName() != null && !node.commonName().isEmpty()) {
                        japiDocument.otherOrganism = node.commonName();
                    } else {
                        japiDocument.otherOrganism = node.scientificName();
                    }
                }
            }
        }
    }

    private void setLineageTaxons(UniProtEntry source, UniProtDocument japiDocument) {
        if (source.hasOrganism()) {
            int taxId = Math.toIntExact(source.getOrganism().getTaxonId());
            setLineageTaxon(taxId, japiDocument);
        }
    }

    private void setOrganismHosts(UniProtEntry source, UniProtDocument japiDocument) {
        List<OrganismHost> hosts = source.getOrganismHosts();

        japiDocument.organismHostIds = extractOrganismHostsIds(hosts);
        japiDocument.organismHostNames = extractOrganismHostsNames(hosts);
    }

    private void convertGeneNames(UniProtEntry source, UniProtDocument japiDocument) {
        if (source.hasGenes()) {
            for (Gene gene : source.getGenes()) {
                addValueToStringList(japiDocument.geneNamesExact, gene.getGeneName());
                addValueListToStringList(japiDocument.geneNamesExact, gene.getSynonyms());
                addValueListToStringList(japiDocument.geneNamesExact, gene.getOrderedLocusNames());
                addValueListToStringList(japiDocument.geneNamesExact, gene.getOrfNames());
            }
        }
    }

    private void convertGoTerms(UniProtEntry source, UniProtDocument japiDocument) {
        List<UniProtDBCrossReference> goTerms = source.getDatabaseCrossReferencesByType("GO");
        if (goTerms != null) {
            for (UniProtDBCrossReference go : goTerms) {
                String goTerm = go.getProperties().stream()
                        .filter(property -> property.getKey().equalsIgnoreCase("GoTerm"))
                        .map(property -> property.getValue().split(":")[1])
                        .collect(Collectors.joining());
                String evType = go.getProperties().stream()
                        .filter(property -> property.getKey().equalsIgnoreCase("GoEvidenceType"))
                        .map(property -> property.getValue().split(":")[0].toLowerCase())
                        .collect(Collectors.joining());
                addGoterm(evType, go.getId(), goTerm, japiDocument);
                addParents(evType, go.getId(), japiDocument);
                addPartOf(evType, go.getId(), japiDocument);
                japiDocument.content.add(go.getId().substring(3));// id
                japiDocument.content.add(goTerm); // term
            }
        }
    }

    private void addParents(String evType, String goId, UniProtDocument japiDocument) {
        List<GoTerm> parents = goRelationRepo.getIsA(goId);
        if (parents.isEmpty())
            return;
        parents.forEach(term -> processGoterm(evType, term, japiDocument));
    }

    private void addPartOf(String evType, String goId, UniProtDocument japiDocument) {
        List<GoTerm> partOf = goRelationRepo.getPartOf(goId);
        if (partOf.isEmpty())
            return;
        partOf.forEach(term -> processGoterm(evType, term, japiDocument));
    }

    private void processGoterm(String evType, GoTerm term, UniProtDocument japiDocument) {
        addGoterm(evType, term.getId(), term.getName(), japiDocument);
        addParents(evType, term.getId(), japiDocument);
        addPartOf(evType, term.getId(), japiDocument);
    }

    private void addGoterm(String evType, String goId, String term, UniProtDocument japiDocument) {
        String key = GO + evType;
        Collection<String> values = japiDocument.goWithEvidenceMaps.get(key);
        if (values == null) {
            values = new HashSet<>();
            japiDocument.goWithEvidenceMaps.put(key, values);
        }
        values.add(goId.substring(3));
        values.add(term);

        japiDocument.goes.add(goId.substring(3));
        japiDocument.goes.add(term);
        japiDocument.goIds.add(goId.substring(3));
    }

    private void convertRcs(List<ReferenceComment> referenceComments, UniProtDocument japiDocument) {
        referenceComments.forEach(referenceComment -> {
            if (referenceComment.hasValue()) {
                switch (referenceComment.getType()) {
                    case STRAIN:
                        japiDocument.rcStrain.add(referenceComment.getValue());
                        break;
                    case TISSUE:
                        japiDocument.rcTissue.add(referenceComment.getValue());
                        break;
                    case PLASMID:
                        japiDocument.rcPlasmid.add(referenceComment.getValue());
                        break;
                    case TRANSPOSON:
                        japiDocument.rcTransposon.add(referenceComment.getValue());
                        break;
                }
            }
        });
    }

    private void convertRp(UniProtReference uniProtReference, UniProtDocument japiDocument) {
        if(uniProtReference.hasReferencePositions()){
            japiDocument.scopes.addAll(uniProtReference.getReferencePositions());
        }
    }

    private void convertReferences(UniProtEntry source, UniProtDocument japiDocument) {
        for (UniProtReference reference : source.getReferences()) {
            Citation citation = reference.getCitation();
            if(reference.hasReferenceComments()) {
                convertRcs(reference.getReferenceComments(), japiDocument);
            }
            convertRp(reference, japiDocument);
            if (citation.hasTitle()) {
                japiDocument.referenceTitles.add(citation.getTitle());
            }
            if (citation.hasAuthors()) {
                japiDocument.referenceAuthors.add(raLineBuilder.buildLine(citation.getAuthors(), false, false).get(0));
            }
            if (citation.hasAuthoringGroup()) {
                japiDocument.referenceOrganizations
                        .add(rgLineBuilder.buildLine(citation.getAuthoringGroup(), false, false).get(0));
            }
            if (citation.hasPublicationDate()) {
                String pubDate = citation.getPublicationDate().getValue();
                try {
                    PublicationDateFormatter dateFormatter = null;
                    if (PublicationDateFormatter.DAY_DIGITMONTH_YEAR.isValidDate(pubDate)) {
                        dateFormatter = PublicationDateFormatter.DAY_DIGITMONTH_YEAR;
                    } else if (PublicationDateFormatter.DAY_THREE_LETTER_MONTH_YEAR.isValidDate(pubDate)) {
                        dateFormatter = PublicationDateFormatter.DAY_THREE_LETTER_MONTH_YEAR;
                    } else if (PublicationDateFormatter.YEAR_DIGIT_MONTH.isValidDate(pubDate)) {
                        dateFormatter = PublicationDateFormatter.YEAR_DIGIT_MONTH;
                    } else if (PublicationDateFormatter.THREE_LETTER_MONTH_YEAR.isValidDate(pubDate)) {
                        dateFormatter = PublicationDateFormatter.THREE_LETTER_MONTH_YEAR;
                    } else if (PublicationDateFormatter.YEAR.isValidDate(pubDate)) {
                        dateFormatter = PublicationDateFormatter.YEAR;
                    }
                    if (dateFormatter != null) {
                        japiDocument.referenceDates.add(dateFormatter.convertStringToDate(pubDate));
                    }
                } catch (ParseException e) {
                    LOGGER.warn("There was a problem converting entry dates during indexing:", e);
                }
            }
            if (citation.getCitationXrefsByType(CitationXrefType.PUBMED).isPresent()) {
                DBCrossReference<CitationXrefType> pubmed = citation.getCitationXrefsByType(CitationXrefType.PUBMED).get();
                japiDocument.referencePubmeds.add(pubmed.getId());
            }
            if (citation instanceof JournalArticle) {
                JournalArticle ja = (JournalArticle) citation;
                japiDocument.referenceJournals.add(ja.getJournal().getName());
            }
        }
    }

    private void convertComment(UniProtEntry source, UniProtDocument japiDocument) {
        for (Comment comment : source.getComments()) {
            if (comment.getCommentType() == CommentType.COFACTOR) {
                convertFactor((CofactorComment) comment, japiDocument);

            }
            if (comment.getCommentType() == CommentType.BIOPHYSICOCHEMICAL_PROPERTIES) {
                convertCommentBPCP((BPCPComment) comment, japiDocument);

            }
            if (comment.getCommentType() == CommentType.SUBCELLULAR_LOCATION) {
                convertCommentSL((SubcellularLocationComment) comment, japiDocument);

            }
            if (comment.getCommentType() == CommentType.ALTERNATIVE_PRODUCTS) {
                convertCommentAP((AlternativeProductsComment) comment, japiDocument);

            }
            if (comment.getCommentType() == CommentType.SEQUENCE_CAUTION) {
                convertCommentSC((SequenceCautionComment) comment, japiDocument);
            }
            if (comment.getCommentType() == CommentType.INTERACTION) {
                convertCommentInteraction((InteractionComment) comment, japiDocument);
            }

            if (comment.getCommentType() == CommentType.SIMILARITY) {
                convertCommentFamily((FreeTextComment) comment, japiDocument);
            }

            if (comment.getCommentType() == CommentType.PATHWAY) {
                convertPathway((FreeTextComment) comment, japiDocument);
            }

            FFLineBuilder<Comment> fbuilder = CCLineBuilderFactory.create(comment);
            String field = getCommentField(comment);
            String evField = getCommentEvField(comment);
            Collection<String> value = japiDocument.commentMap.computeIfAbsent(field, k -> new ArrayList<>());


            //@lgonzales doubt: can we change to without evidence??? because we already have the evidence map
            String commentVal = fbuilder.buildStringWithEvidence(comment);
            value.add(commentVal);
            japiDocument.content.add(commentVal);

            Collection<String> evValue = japiDocument.commentEvMap.computeIfAbsent(evField, k -> new HashSet<>());
            Set<String> evidences = fetchEvidences(comment);
            evValue.addAll(evidences);
        }
    }

    private Set<String> fetchEvidences(Comment comment) {
        Set<String> evidences = new HashSet<>();
        if (comment instanceof FreeTextComment) {
            FreeTextComment toComment = (FreeTextComment) comment;
            evidences.addAll(extractEvidence(toComment.getTexts().stream().flatMap(val -> val.getEvidences().stream())
                                                     .collect(Collectors.toList())));
        }
        CommentType type = comment.getCommentType();
        switch (type) {
            case DISEASE:
                DiseaseComment diseaseComment = (DiseaseComment) comment;
                if(diseaseComment.hasDefinedDisease()) {
                    evidences.addAll(extractEvidence(
                            diseaseComment.getDisease().getEvidences()));
                    if(diseaseComment.hasNote() && diseaseComment.getNote().hasTexts()) {
                        evidences.addAll(extractEvidence(diseaseComment.getNote().getTexts().stream()
                                                                 .flatMap(val -> val.getEvidences().stream())
                                                                 .collect(Collectors.toList())));
                    }
                }
                break;
            case RNA_EDITING:
                RnaEditingComment reComment = (RnaEditingComment) comment;
                if(reComment.hasPositions()) {
                    evidences.addAll(extractEvidence(reComment.getPositions().stream()
                                                             .flatMap(val -> val.getEvidences().stream())
                                                             .collect(Collectors.toList())));
                }
                if(reComment.hasNote()) {
                    evidences.addAll(extractEvidence(reComment.getNote().getTexts().stream()
                                                             .flatMap(val -> val.getEvidences().stream())
                                                             .collect(Collectors.toList())));
                }
                break;
            case MASS_SPECTROMETRY:
                MassSpectrometryComment msComment = (MassSpectrometryComment) comment;
                evidences.addAll(extractEvidence(msComment.getEvidences()));
                break;
        }
        return evidences;
    }

    private void convertCommentInteraction(InteractionComment comment, UniProtDocument japiDocument) {
        comment.getInteractions().forEach(interaction -> {
            if(interaction.hasFirstInteractor()) {
                japiDocument.interactors.add(interaction.getFirstInteractor().getValue());
            }
            if(interaction.hasSecondInteractor()) {
                japiDocument.interactors.add(interaction.getSecondInteractor().getValue());
            }
            if(interaction.hasUniProtAccession()) {
                japiDocument.interactors.add(interaction.getUniProtAccession().getValue());
            }
        });
    }

    private void convertCommentFamily(FreeTextComment comment, UniProtDocument japiDocument) {
        comment.getTexts().stream()
                .map(Value::getValue)
                .forEach(val -> updateFamily(val, japiDocument));
    }

    private void convertPathway(FreeTextComment comment, UniProtDocument japiDocument) {
        comment.getTexts().stream()
                .map(Value::getValue)
                .forEach(val -> updatePathway(val, japiDocument));
    }
    private void updatePathway(String val, UniProtDocument japiDocument) {
        UniPathway unipathway = pathwayRepo.getFromName(val);
        if(unipathway !=null) {
            japiDocument.pathway.add(unipathway.getAccession());
        }
    }
    private void updateFamily(String val, UniProtDocument japiDocument) {
        if (!val.endsWith(".")) {
            val += ".";
        }
        Matcher m = PATTERN_FAMILY.matcher(val);
        if (m.matches()) {
            StringBuilder line = new StringBuilder();
            line.append(m.group(1));
            if (m.group(2) != null)
                line.append(", ").append(m.group(2));
            if (m.group(3) != null)
                line.append(", ").append(m.group(3));
            japiDocument.familyInfo.add(line.toString());
        }
    }

    private void convertCommentSC(SequenceCautionComment comment, UniProtDocument japiDocument) {
        String val = comment.getSequenceCautionType().toDisplayName();
        if (comment.hasNote()) {
            val = comment.getNote();
        }
        Set<String> evidence = extractEvidence(comment.getEvidences());
        japiDocument.seqCaution.add(val);
        japiDocument.seqCautionEv.addAll(evidence);
        switch (comment.getSequenceCautionType()) {
            case FRAMESHIFT:
                japiDocument.seqCautionFrameshift.add(val);
                // japiDocument.seqCautionFrameshiftEv.addAll(evidence);
                break;
            case ERRONEOUS_INITIATION:
                japiDocument.seqCautionErInit.add(val);
                // japiDocument.seqCautionErInitEv.addAll(evidence);
                break;
            case ERRONEOUS_TERMIINATION:
                japiDocument.seqCautionErTerm.add(val);
                // japiDocument.seqCautionErTermEv.addAll(evidence);
                break;
            case ERRONEOUS_PREDICTION:
                japiDocument.seqCautionErPred.add(val);
                // japiDocument.seqCautionErPredEv.addAll(evidence);
                break;
            case ERRONEOUS_TRANSLATION:
                japiDocument.seqCautionErTran.add(val);
                // japiDocument.seqCautionErTranEv.addAll(evidence);
                break;
            case MISCELLANEOUS_DISCREPANCY:
                japiDocument.seqCautionMisc.add(val);
                japiDocument.seqCautionMiscEv.addAll(evidence);
                break;

            default:

        }

    }

    private void convertCommentBPCP(BPCPComment comment, UniProtDocument japiDocument) {
        if (comment.hasAbsorption()) {
            Absorption absorption = comment.getAbsorption();
            japiDocument.bpcpAbsorption.add("" + absorption.getMax());
            japiDocument.bpcpAbsorptionEv.addAll(extractEvidence(absorption.getEvidences()));
            if (absorption.getNote() != null) {
                japiDocument.bpcpAbsorption.addAll(absorption.getNote().getTexts().stream().map(Value::getValue)
                                                           .collect(Collectors.toList()));
                japiDocument.bpcpAbsorptionEv.addAll(extractEvidence(absorption.getNote().getTexts().stream()
                                                                             .flatMap(val -> val.getEvidences()
                                                                                     .stream())
                                                                             .collect(Collectors.toList())));
            }
            japiDocument.bpcp.addAll(japiDocument.bpcpAbsorption);
            japiDocument.bpcpEv.addAll(japiDocument.bpcpAbsorptionEv);
        }
        if (comment.hasKineticParameters()) {
            KineticParameters kp = comment.getKineticParameters();
            kp.getMaximumVelocities().stream().map(MaximumVelocity::getEnzyme)
                    .filter(val -> !Strings.isNullOrEmpty(val)).forEach(japiDocument.bpcpKinetics::add);
            japiDocument.bpcpKineticsEv.addAll(extractEvidence(kp.getMaximumVelocities().stream()
                                                                       .flatMap(val -> val.getEvidences().stream())
                                                                       .collect(Collectors.toList())));
            kp.getMichaelisConstants().stream().map(MichaelisConstant::getSubstrate)
                    .filter(val -> !Strings.isNullOrEmpty(val)).forEach(japiDocument.bpcpKinetics::add);
            japiDocument.bpcpKineticsEv.addAll(extractEvidence(kp.getMichaelisConstants().stream()
                                                                       .flatMap(val -> val.getEvidences().stream())
                                                                       .collect(Collectors.toList())));
            if (kp.getNote() != null) {
                japiDocument.bpcpKinetics.addAll(
                        kp.getNote().getTexts().stream().map(Value::getValue).collect(Collectors.toList()));
                japiDocument.bpcpKineticsEv.addAll(extractEvidence(kp.getNote().getTexts().stream()
                                                                           .flatMap(val -> val.getEvidences()
                                                                                   .stream())
                                                                           .collect(Collectors.toList())));
            }
            if (japiDocument.bpcpKinetics.isEmpty()) {
                kp.getMaximumVelocities().stream().map(val -> "" + val.getVelocity())
                        .forEach(japiDocument.bpcpKinetics::add);
                kp.getMichaelisConstants().stream().map(val -> "" + val.getConstant())
                        .forEach(japiDocument.bpcpKinetics::add);
            }
            japiDocument.bpcp.addAll(japiDocument.bpcpKinetics);
            japiDocument.bpcpEv.addAll(japiDocument.bpcpKineticsEv);

        }
        if (comment.hasPhDependence()) {
            comment.getPhDependence().getTexts().stream().map(Value::getValue)
                    .forEach(japiDocument.bpcpPhDependence::add);
            japiDocument.bpcpPhDependenceEv.addAll(extractEvidence(comment.getPhDependence().getTexts().stream()
                                                                           .flatMap(val -> val.getEvidences()
                                                                                   .stream())
                                                                           .collect(Collectors.toList())));
            japiDocument.bpcp.addAll(japiDocument.bpcpPhDependence);
            japiDocument.bpcpEv.addAll(japiDocument.bpcpPhDependenceEv);
        }
        if (comment.hasRedoxPotential()) {
            comment.getRedoxPotential().getTexts().stream().map(Value::getValue)
                    .forEach(japiDocument.bpcpRedoxPotential::add);
            japiDocument.bpcpRedoxPotentialEv.addAll(extractEvidence(comment.getRedoxPotential().getTexts().stream()
                                                                             .flatMap(val -> val.getEvidences()
                                                                                     .stream())
                                                                             .collect(Collectors.toList())));
            japiDocument.bpcp.addAll(japiDocument.bpcpRedoxPotential);
            japiDocument.bpcpEv.addAll(japiDocument.bpcpRedoxPotentialEv);
        }
        if (comment.hasTemperatureDependence()) {
            comment.getTemperatureDependence().getTexts().stream().map(Value::getValue)
                    .forEach(japiDocument.bpcpTempDependence::add);
            japiDocument.bpcpTempDependenceEv.addAll(extractEvidence(comment.getTemperatureDependence().getTexts()
                                                                             .stream()
                                                                             .flatMap(val -> val.getEvidences()
                                                                                     .stream())
                                                                             .collect(Collectors.toList())));
            japiDocument.bpcp.addAll(japiDocument.bpcpTempDependence);
            japiDocument.bpcpEv.addAll(japiDocument.bpcpTempDependenceEv);
        }
    }

    private void convertCommentSL(SubcellularLocationComment comment, UniProtDocument japiDocument) {
        if(comment.hasSubcellularLocations()){
            comment.getSubcellularLocations().forEach(subcellularLocation -> {
                if (subcellularLocation.hasLocation()) {
                    japiDocument.subcellLocationTerm.add(subcellularLocation.getLocation().getValue());

                    Set<String> locationEv = extractEvidence(subcellularLocation.getLocation().getEvidences());
                    japiDocument.subcellLocationTermEv.addAll(locationEv);

                }
                if (subcellularLocation.hasOrientation()) {
                    japiDocument.subcellLocationTerm.add(subcellularLocation.getOrientation().getValue());

                    Set<String> orientationEv = extractEvidence(subcellularLocation.getOrientation().getEvidences());
                    japiDocument.subcellLocationTermEv.addAll(orientationEv);
                }
                if (subcellularLocation.hasTopology()) {
                    japiDocument.subcellLocationTerm.add(subcellularLocation.getTopology().getValue());

                    Set<String> topologyEv = extractEvidence(subcellularLocation.getTopology().getEvidences());
                    japiDocument.subcellLocationTermEv.addAll(topologyEv);
                }
            });
        }
        if(comment.hasNote()) {
            comment.getNote().getTexts().stream().map(Value::getValue)
                    .forEach(japiDocument.subcellLocationNote::add);
            Set<String> noteEv = extractEvidence(comment.getNote().getTexts().stream()
                                                         .flatMap(val -> val.getEvidences().stream())
                                                         .collect(Collectors.toList()));
            japiDocument.subcellLocationNoteEv.addAll(noteEv);
        }
    }

    private Set<String> extractEvidence(List<Evidence> evidences) {
        Set<String> extracted = evidences.stream().map(ev -> ev.getEvidenceCode().name()).collect(Collectors.toSet());
        if (extracted.stream().anyMatch(val -> MANUAL_EVIDENCE_MAP.contains(val))) {
            extracted.add(MANUAL_EVIDENCE);
        }
        if (extracted.stream().anyMatch(val -> AUTOMATIC_EVIDENCE_MAP.contains(val))) {
            extracted.add(AUTOMATIC_EVIDENCE);
        }
        if (extracted.stream().anyMatch(val -> EXPERIMENTAL_EVIDENCE_MAP.contains(val))) {
            extracted.add(EXPERIMENTAL_EVIDENCE);
        }
        return extracted;
    }

    private void convertCommentAP(AlternativeProductsComment comment, UniProtDocument japiDocument) {
        List<String> values = new ArrayList<>();
        Set<String> evidence =  new HashSet<>();
        if(comment.hasNote()) {
            comment.getNote().getTexts().stream().map(Value::getValue)
                    .forEach(values::add);

            evidence.addAll(extractEvidence(comment.getNote().getTexts().stream()
                                                    .flatMap(val -> val.getEvidences().stream())
                                                    .collect(Collectors.toList())));
        }

        List<String> events = new ArrayList<>();
        if(comment.hasEvents()) {
            comment.getEvents().stream().map(APEventType::getName).forEach(events::add);
            values.addAll(events);
        }

        japiDocument.ap.addAll(values);
        japiDocument.apEv.addAll(evidence);
        for (String event : events) {
            if ("alternative promoter usage".equalsIgnoreCase(event)) {
                japiDocument.apApu.addAll(values);
                japiDocument.apApuEv.addAll(evidence);
            }
            if ("alternative splicing".equalsIgnoreCase(event)) {
                japiDocument.apAs.addAll(values);
                japiDocument.apAsEv.addAll(evidence);
            }
            if ("alternative initiation".equalsIgnoreCase(event)) {
                japiDocument.apAi.addAll(values);
                japiDocument.apAiEv.addAll(evidence);
            }
            if ("ribosomal frameshifting".equalsIgnoreCase(event)) {
                japiDocument.apRf.addAll(values);
                japiDocument.apRfEv.addAll(evidence);
            }
        }

    }

    private void convertFactor(CofactorComment comment, UniProtDocument japiDocument) {
        if(comment.hasCofactors()) {
            comment.getCofactors().forEach(val -> {
                japiDocument.cofactorChebi.add(val.getName());
                if (val.getCofactorReference().getDatabaseType() == CofactorReferenceType.CHEBI) {
                    String id = val.getCofactorReference().getId();
                    if (id.startsWith("CHEBI:"))
                        id = id.substring("CHEBI:".length());
                    japiDocument.cofactorChebi.add(id);
                }
                japiDocument.cofactorChebiEv.addAll(extractEvidence(val.getEvidences()));
            });
        }

        if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
            comment.getNote().getTexts().forEach(val -> {
                japiDocument.cofactorNote.add(val.getValue());
                japiDocument.cofactorNoteEv.addAll(extractEvidence(val.getEvidences()));
            });
        }
    }

    private void convertXref(UniProtEntry source, UniProtDocument japiDocument) {
        boolean d3structure = false;
        for (UniProtDBCrossReference xref : source.getDatabaseCrossReferences()) {
            if (xref.getDatabaseType().getName().equalsIgnoreCase("PDB")) {
                d3structure = true;
            }
            String dbname = xref.getDatabaseType().getName().toLowerCase();
            japiDocument.databases.add(dbname);
            String id = xref.getId();
            if (!dbname.equalsIgnoreCase("PIR")){
                addXrefId(id, dbname, japiDocument.xrefs);
            }
            switch (dbname.toLowerCase()) {
                case "embl":
                    if(xref.hasProperties()) {
                        Optional<String> proteinId = xref.getProperties().stream()
                                .filter(property -> property.getKey().equalsIgnoreCase("ProteinId"))
                                .filter(property -> !property.getValue().equalsIgnoreCase("-"))
                                .map(Property::getValue)
                                .findFirst();
                        proteinId.ifPresent(s -> addXrefId(s, dbname, japiDocument.xrefs));
                    }
                    break;
                case "refseq":
                case "pir":
                case "unipathway":
                case "ensembl":
                    if(xref.hasProperties()) {
                        List<String> properties = xref.getProperties().stream()
                                .filter(property -> !property.getValue().equalsIgnoreCase("-"))
                                .map(Property::getValue)
                                .collect(Collectors.toList());
                        properties.forEach(s -> addXrefId(s, dbname, japiDocument.xrefs));
                    }
                    break;
                case "proteomes":
                    japiDocument.proteomes.add(xref.getId());
                    if(xref.hasProperties()) {
                        japiDocument.proteomeComponents.addAll(xref.getProperties().stream()
                                                                       .map(Property::getValue)
                                                                       .collect(Collectors.toSet()));
                    }
                default:

            }

        }
        japiDocument.d3structure = d3structure;
    }

    private void addXrefId(String id, String dbname, Collection<String> values) {
        values.add(id);
        values.add(dbname + "-" + id);
        if (id.indexOf(".") > 0) {
            String idMain = id.substring(0, id.indexOf("."));
            values.add(idMain);
            values.add(dbname + "-" + idMain);
        }
    }

    private void setOrganelle(UniProtEntry source, UniProtDocument japiDocument) {
        for (GeneLocation geneLocation : source.getGeneLocations()) {
            GeneEncodingType geneEncodingType = geneLocation.getGeneEncodingType();

            if (PLASTID_CHILD.contains(geneEncodingType)) {
                japiDocument.organelles.add(GeneEncodingType.PLASTID.getName());
            }

            String organelleValue = geneEncodingType.getName();
            if (geneLocation.getValue() != null && !geneLocation.getValue().isEmpty()) {
                organelleValue += " " + geneLocation.getValue();
            }
            organelleValue = organelleValue.trim();

            japiDocument.organelles.add(organelleValue);
        }
    }

    private void convertFeature(UniProtEntry source, UniProtDocument japiDocument) {
        for (Feature feature : source.getFeatures()) {
            FFLineBuilder<Feature> fbuilder = FeatureLineBuilderFactory.create(feature);

            String field = getFeatureField(feature, FEATURE);
            String evField = getFeatureField(feature, FT_EV);
            String lengthField = getFeatureField(feature, FT_LENGTH);
            Collection<String> featuresOfTypeList = japiDocument.featuresMap.computeIfAbsent(field, k -> new HashSet<>());
            String featureText = fbuilder.buildStringWithEvidence(feature);
            featuresOfTypeList.add(featureText);
            japiDocument.content.add(featureText);

            // start and end of location
            int length = feature.getLocation().getEnd().getValue() - feature.getLocation().getStart().getValue() + 1;
            Set<String> evidences = extractEvidence(feature.getEvidences());
            Collection<Integer> lengthList = japiDocument.featureLengthMap.computeIfAbsent(lengthField, k -> new HashSet<>());
            lengthList.add(length);

            Collection<String> evidenceList = japiDocument.featureEvidenceMap.computeIfAbsent(evField, k -> new HashSet<>());
            evidenceList.addAll(evidences);
        }
    }

    private void setFragmentNPrecursor(UniProtEntry source, UniProtDocument japiDocument) {
        ProteinDescription description = source.getProteinDescription();
        boolean isFragment = false;
        boolean isPrecursor = false;
        if (source.hasProteinDescription() && description.hasFlag()) {
            Flag flag = description.getFlag();
            switch (flag.getType()){
                case FRAGMENT:
                case FRAGMENTS:
                    isFragment = true;
                    break;
                case PRECURSOR:
                    isPrecursor = true;
                    break;
                case FRAGMENT_PRECURSOR:
                case FRAGMENTS_PRECURSOR:
                    isPrecursor = true;
                    isFragment = true;
                    break;
            }
        }
        japiDocument.fragment = isFragment;
        japiDocument.precursor = isPrecursor;
    }

    private void setProteinExistence(UniProtEntry source, UniProtDocument japiDocument) {
        ProteinExistence proteinExistence = source.getProteinExistence();
        if (proteinExistence != null) {
            japiDocument.proteinExistence = proteinExistence.name();
        }
    }

    private List<String> extractProteinDescriptionEcs(ProteinDescription proteinDescription) {
        List<String> ecs = new ArrayList<>();
        if(proteinDescription.hasRecommendedName() && proteinDescription.getRecommendedName().hasEcNumbers()){
            ecs.addAll(getEcs(proteinDescription.getRecommendedName().getEcNumbers()));
        }
        if(proteinDescription.hasSubmissionNames()){
            proteinDescription.getSubmissionNames().stream()
                    .filter(ProteinSubName::hasEcNumbers)
                    .flatMap(proteinSubName -> getEcs(proteinSubName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        if(proteinDescription.hasAlternativeNames()){
            proteinDescription.getAlternativeNames().stream()
                    .filter(ProteinAltName::hasEcNumbers)
                    .flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        if(proteinDescription.hasContains()){
            proteinDescription.getContains().stream()
                    .flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream())
                    .forEach(ecs::add);
        }
        if(proteinDescription.hasIncludes()){
            proteinDescription.getIncludes().stream()
                    .flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream())
                    .forEach(ecs::add);
        }
        return ecs;
    }

    private List<String> getProteinSectionEcs(ProteinSection proteinSection){
        List<String> ecs = new ArrayList<>();
        if(proteinSection.hasRecommendedName() && proteinSection.getRecommendedName().hasEcNumbers()){
            ecs.addAll(getEcs(proteinSection.getRecommendedName().getEcNumbers()));
        }
        if(proteinSection.hasAlternativeNames()){
            proteinSection.getAlternativeNames().stream()
                    .filter(ProteinAltName::hasEcNumbers)
                    .flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        return ecs;
    }

    private List<String> getEcs(List<EC> ecs){
        return ecs.stream()
                .map(EC::getValue)
                .collect(Collectors.toList());
    }

    private List<String> extractProteinDescriptionValues(ProteinDescription description){
        List<String> values = new ArrayList<>();
        if(description.hasRecommendedName()){
            values.addAll(getProteinRecNameNames(description.getRecommendedName()));
        }
        if(description.hasSubmissionNames()){
            description.getSubmissionNames().stream()
                    .map(this::getProteinSubNameNames)
                    .forEach(values::addAll);
        }
        if(description.hasAlternativeNames()){
            description.getAlternativeNames().stream()
                    .map(this::getProteinAltNameNames)
                    .forEach(values::addAll);
        }
        if(description.hasContains()){
            description.getContains().stream()
                    .map(this::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if(description.hasIncludes()){
            description.getIncludes().stream()
                    .map(this::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if(description.hasAllergenName()){
            values.add(description.getAllergenName().getValue());
        }
        if(description.hasBiotechName()){
            values.add(description.getBiotechName().getValue());
        }
        if(description.hasCdAntigenNames()){
            description.getCdAntigenNames().stream()
                    .map(Value::getValue)
                    .forEach(values::add);
        }
        if(description.hasInnNames()){
            description.getInnNames().stream()
                    .map(Value::getValue)
                    .forEach(values::add);
        }
        return values;
    }

    private List<String> getProteinSectionValues(ProteinSection proteinSection){
        List<String> names = new ArrayList<>();
        if(proteinSection.hasRecommendedName()){
            names.addAll(getProteinRecNameNames(proteinSection.getRecommendedName()));
        }
        if(proteinSection.hasAlternativeNames()){
            proteinSection.getAlternativeNames().stream()
                    .map(this::getProteinAltNameNames)
                    .forEach(names::addAll);
        }
        if(proteinSection.hasCdAntigenNames()){
            proteinSection.getCdAntigenNames().stream()
                    .map(Value::getValue)
                    .forEach(names::add);
        }
        if(proteinSection.hasAllergenName()){
            names.add(proteinSection.getAllergenName().getValue());
        }
        if(proteinSection.hasInnNames()){
            proteinSection.getInnNames().stream()
                    .map(Value::getValue)
                    .forEach(names::add);
        }
        if(proteinSection.hasBiotechName()){
            names.add(proteinSection.getBiotechName().getValue());
        }
        return names;
    }

    private List<String> getProteinRecNameNames(ProteinRecName proteinRecName){
        List<String> names = new ArrayList<>();
        if(proteinRecName.hasFullName()){
            names.add(proteinRecName.getFullName().getValue());
        }
        if(proteinRecName.hasShortNames()){
            proteinRecName.getShortNames()
                    .stream()
                    .map(Name::getValue)
                    .forEach(names::add);
        }
        return names;
    }

    private List<String> getProteinAltNameNames(ProteinAltName proteinAltName){
        List<String> names = new ArrayList<>();
        if(proteinAltName.hasShortNames()){
            proteinAltName.getShortNames()
                    .stream()
                    .map(Name::getValue)
                    .forEach(names::add);
        }
        if(proteinAltName.hasFullName()){
            names.add(proteinAltName.getFullName().getValue());
        }
        return names;
    }

    private List<String> getProteinSubNameNames(ProteinSubName proteinAltName){
        List<String> names = new ArrayList<>();
        if(proteinAltName.hasFullName()){
            names.add(proteinAltName.getFullName().getValue());
        }
        return names;
    }

    private void setLineageTaxon(int taxId, UniProtDocument japiDocument) {
        if (taxId > 0) {
            Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);

            while (taxonomicNode.isPresent()) {
                TaxonomicNode node = taxonomicNode.get();
                japiDocument.taxLineageIds.add(node.id());
                japiDocument.organismTaxon.addAll(extractTaxonode(node));
                taxonomicNode = getParentTaxon(node.id());
            }
        }
    }

    private List<String> extractTaxonode(TaxonomicNode node) {
        List<String> taxonmyItems = new ArrayList<>();
        if (node.scientificName() != null && !node.scientificName().isEmpty()) {
            taxonmyItems.add(node.scientificName());
        }
        if (node.commonName() != null && !node.commonName().isEmpty()) {
            taxonmyItems.add(node.commonName());
        }
        if (node.synonymName() != null && !node.synonymName().isEmpty()) {
            taxonmyItems.add(node.synonymName());
        }
        if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
            taxonmyItems.add(node.mnemonic());
        }
        return taxonmyItems;
    }

    private List<Integer> extractOrganismHostsIds(Collection<OrganismHost> hosts) {
        return hosts.stream()
                .map(ncbiId -> Math.toIntExact(ncbiId.getTaxonId()))
                .collect(Collectors.toList());
    }

    private List<String> extractOrganismHostsNames(Collection<OrganismHost> hosts) {
        return hosts.stream()
                .map(ncbiId -> taxonomyRepo.retrieveNodeUsingTaxID(Math.toIntExact(ncbiId.getTaxonId())))
                .filter(Optional::isPresent)
                .flatMap(node -> extractTaxonode(node.get()).stream())
                .collect(Collectors.toList());
    }

    private String getCommentField(Comment c) {
        String field = COMMENT + c.getCommentType().name().toLowerCase();
        return field.replaceAll(" ", "_");
    }

    private String getCommentEvField(Comment c) {
        String field = CC_EV + c.getCommentType().name().toLowerCase();
        return field.replaceAll(" ", "_");
    }

    private String getFeatureField(Feature feature, String type) {
        String field = type + feature.getType().name().toLowerCase();
        return field.replaceAll(" ", "_");
    }

    private Optional<TaxonomicNode> getParentTaxon(int taxId) {
        Optional<TaxonomicNode> optionalNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);
        return optionalNode.filter(TaxonomicNode::hasParent).map(TaxonomicNode::parent);
    }
}
