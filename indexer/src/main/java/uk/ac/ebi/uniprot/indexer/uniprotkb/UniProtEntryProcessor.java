package uk.ac.ebi.uniprot.indexer.uniprotkb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.common.PublicationDateFormatter;
import uk.ac.ebi.uniprot.cv.keyword.KeywordDetail;
import uk.ac.ebi.uniprot.cv.pathway.UniPathway;
import uk.ac.ebi.uniprot.domain.Property;
import uk.ac.ebi.uniprot.domain.Sequence;
import uk.ac.ebi.uniprot.domain.Value;
import uk.ac.ebi.uniprot.domain.citation.Citation;
import uk.ac.ebi.uniprot.domain.citation.CitationXrefType;
import uk.ac.ebi.uniprot.domain.citation.JournalArticle;
import uk.ac.ebi.uniprot.domain.gene.Gene;
import uk.ac.ebi.uniprot.domain.impl.SequenceImpl;
import uk.ac.ebi.uniprot.domain.taxonomy.OrganismHost;
import uk.ac.ebi.uniprot.domain.uniprot.*;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtAccessionBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtEntryBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtIdBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.comment.*;
import uk.ac.ebi.uniprot.domain.uniprot.description.*;
import uk.ac.ebi.uniprot.domain.uniprot.evidence.Evidence;
import uk.ac.ebi.uniprot.domain.uniprot.feature.Feature;
import uk.ac.ebi.uniprot.domain.uniprot.xdb.UniProtDBCrossReference;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.FFLineBuilder;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.flatfile.parser.impl.cc.CCLineBuilderFactory;
import uk.ac.ebi.uniprot.flatfile.parser.impl.ft.FeatureLineBuilderFactory;
import uk.ac.ebi.uniprot.flatfile.parser.impl.ra.RALineBuilder;
import uk.ac.ebi.uniprot.flatfile.parser.impl.rg.RGLineBuilder;
import uk.ac.ebi.uniprot.indexer.common.DocumentConversionException;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTerm;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomicNode;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.indexer.util.DateUtils;
import uk.ac.ebi.uniprot.json.parser.uniprot.UniprotJsonConfig;
import uk.ebi.uniprot.scorer.uniprotkb.UniProtEntryScored;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryProcessor implements ItemProcessor<ConvertableEntry, ConvertableEntry> {
    private static final Logger INDEXING_FAILED_LOGGER = LoggerFactory.getLogger("indexing-doc-conversion-failed-entries");
    static final int SORT_FIELD_MAX_LENGTH = 30;
    static final int MAX_STORED_FIELD_LENGTH = 32766;

    private static final String XREF = "xref_";
    private static final String COMMENT = "cc_";
    private static final String CC_EV = "ccev_";
    private static final String FEATURE = "ft_";
    private static final String FT_EV = "ftev_";
    private static final String FT_LENGTH = "ftlen_";
    private static final String DASH = "-";
    private static final String GO = "go_";
    private static final Pattern PATTERN_FAMILY = Pattern
            .compile("(?:In the .+? section; )?[Bb]elongs to the (.+?family)\\.(?: (.+?family)\\.)?(?: (.+?family)\\.)?(?: Highly divergent\\.)?");
    /**
     * An enum set representing all of the organelles that are children of plastid
     */
    private static final EnumSet<GeneEncodingType> PLASTID_CHILD = EnumSet.of(GeneEncodingType.APICOPLAST_PLASTID,
                                                                              GeneEncodingType.CHLOROPLAST_PLASTID, GeneEncodingType.CYANELLE_PLASTID,
                                                                              GeneEncodingType.NON_PHOTOSYNTHETIC_PLASTID, GeneEncodingType.CHROMATOPHORE_PLASTID);
    private static final String MANUAL_EVIDENCE = "manual";
    private static final List<String> MANUAL_EVIDENCE_MAP = asList("ECO_0000269", "ECO_0000303", "ECO_0000305",
                                                                   "ECO_0000250", "ECO_0000255", "ECO_0000244", "ECO_0000312");
    private static final String AUTOMATIC_EVIDENCE = "automatic";
    private static final List<String> AUTOMATIC_EVIDENCE_MAP = asList("ECO_0000256", "ECO_0000213",
                                                                      "ECO_0000313", "ECO_0000259");
    private static final String EXPERIMENTAL_EVIDENCE = "experimental";
    private static final List<String> EXPERIMENTAL_EVIDENCE_MAP = singletonList("ECO_0000269");
    private static Map<Integer, String> popularOrganismsTaxName;

    static {
        Map<Integer, String> popularOrganismsMap = new HashMap<>();
        popularOrganismsMap.put(9606, "Human");
        popularOrganismsMap.put(10090, "Mouse");
        popularOrganismsMap.put(10116, "Rat");
        popularOrganismsMap.put(9913, "Bovine");
        popularOrganismsMap.put(7955, "Zebrafish");
        popularOrganismsMap.put(7227, "Fruit fly");
        popularOrganismsMap.put(6239, "C. elegans");
        popularOrganismsMap.put(44689, "Slime mold");
        popularOrganismsMap.put(3702, "A. thaliana");
        popularOrganismsMap.put(39947, "Rice");
        popularOrganismsMap.put(83333, "E. coli K12");
        popularOrganismsMap.put(224308, "B. subtilis");
        popularOrganismsMap.put(559292, "S. cerevisiae");
        popularOrganismsTaxName = Collections.unmodifiableMap(popularOrganismsMap);
    }

    private final TaxonomyRepo taxonomyRepo;
    private final RALineBuilder raLineBuilder;
    private final RGLineBuilder rgLineBuilder;
    private final GoRelationRepo goRelationRepo;
    private final KeywordRepo keywordRepo;
    private final PathwayRepo pathwayRepo;

    public UniProtEntryProcessor(TaxonomyRepo taxonomyRepo,
                                 GoRelationRepo goRelationRepo,
                                 KeywordRepo keywordRepo,
                                 PathwayRepo pathwayRepo) {
        this.taxonomyRepo = taxonomyRepo;
        this.goRelationRepo = goRelationRepo;
        this.keywordRepo = keywordRepo;
        this.pathwayRepo = pathwayRepo;
        this.raLineBuilder = new RALineBuilder();
        this.rgLineBuilder = new RGLineBuilder();
    }

    @Override
    public ConvertableEntry process(ConvertableEntry convertableEntry) {
        UniProtEntry uniProtEntry = convertableEntry.getEntry();
        try {
            UniProtDocument doc = new UniProtDocument();
            convertableEntry.convertsTo(doc);

            doc.accession = uniProtEntry.getPrimaryAccession().getValue();
            if (doc.accession.contains(DASH)) {
                if (isCanonicalIsoform(uniProtEntry)) {
                    doc.reviewed = null;
                    doc.isIsoform = null;
                    return convertableEntry;
                }
                doc.isIsoform = true;
                // We are adding the canonical accession to the isoform entry as a secondary accession.
                // this way when you search by an accession and ask to include isoforms, it will find it.
                String canonicalAccession = doc.accession.substring(0, doc.accession.indexOf(DASH));
                doc.secacc.add(canonicalAccession);
            } else {
                doc.isIsoform = false;
            }

            doc.id = uniProtEntry.getUniProtId().getValue();
            addValueListToStringList(doc.secacc, uniProtEntry.getSecondaryAccessions());

            setReviewed(uniProtEntry, doc);
            setAuditInfo(uniProtEntry, doc);
            setProteinNames(uniProtEntry, doc);
            setECNumbers(uniProtEntry, doc);
            setKeywords(uniProtEntry, doc);
            setOrganism(uniProtEntry, doc);
            setLineageTaxons(uniProtEntry, doc);
            setOrganismHosts(uniProtEntry, doc);
            convertGeneNames(uniProtEntry, doc);
            convertGoTerms(uniProtEntry, doc);
            convertReferences(uniProtEntry, doc);
            convertComment(uniProtEntry, doc);
            convertXref(uniProtEntry, doc);
            setOrganelle(uniProtEntry, doc);
            convertFeature(uniProtEntry, doc);
            setFragmentNPrecursor(uniProtEntry, doc);
            setProteinExistence(uniProtEntry, doc);
            setSequence(uniProtEntry, doc);
            setScore(uniProtEntry, doc);
            setAvroDefaultEntry(uniProtEntry, doc);
            setDefaultSearchContent(doc);
            return convertableEntry;
        } catch (IllegalArgumentException | NullPointerException e) {
            writeFailedEntryToFile(uniProtEntry);
            String acc = uniProtEntry.getPrimaryAccession().getValue();
            throw new DocumentConversionException("Error occurred whilst trying to convert UniProt entry: " + acc, e);
        }
    }

    private void writeFailedEntryToFile(UniProtEntry uniProtEntry) {
        String entryFF = UniProtFlatfileWriter.write(uniProtEntry);
        INDEXING_FAILED_LOGGER.error(entryFF);
    }

    static String truncatedSortValue(String value) {
        if (value.length() > SORT_FIELD_MAX_LENGTH) {
            return value.substring(0, SORT_FIELD_MAX_LENGTH);
        } else {
            return value;
        }
    }

    static String getDefaultBinaryValue(String base64Value) {
        if (base64Value.length() <= MAX_STORED_FIELD_LENGTH) {
            return base64Value;
        } else {
            return null;
        }
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

    private void setReviewed(UniProtEntry uniProtEntry, UniProtDocument doc) {
        doc.reviewed = (uniProtEntry.getEntryType() == UniProtEntryType.SWISSPROT);
    }

    private void setAuditInfo(UniProtEntry uniProtEntry, UniProtDocument doc) {
        EntryAudit entryAudit = uniProtEntry.getEntryAudit();
        if (entryAudit != null) {
            doc.firstCreated = DateUtils.convertLocalDateToDate(entryAudit.getFirstPublicDate());
            doc.lastModified = DateUtils.convertLocalDateToDate(entryAudit.getLastAnnotationUpdateDate());
            doc.sequenceUpdated = DateUtils.convertLocalDateToDate(entryAudit.getLastSequenceUpdateDate());
        }
    }

    private void setDefaultSearchContent(UniProtDocument doc) {
        doc.content.add(doc.accession);
        doc.content.addAll(doc.secacc);
        doc.content.add(doc.id); //mnemonic

        doc.content.addAll(doc.proteinNames);
        doc.content.addAll(doc.keywords);
        doc.content.addAll(doc.geneNames);

        doc.content.add(String.valueOf(doc.organismTaxId));
        doc.content.addAll(doc.organismName);
        doc.content.addAll(doc.organismHostNames);
        doc.content
                .addAll(doc.organismHostIds.stream().map(String::valueOf).collect(Collectors.toList()));

        doc.content.addAll(doc.organelles);

        doc.content.addAll(doc.referenceAuthors);
        doc.content.addAll(doc.referenceJournals);
        doc.content.addAll(doc.referenceOrganizations);
        doc.content.addAll(doc.referencePubmeds);
        doc.content.addAll(doc.referenceTitles);

        doc.content.addAll(doc.proteomes);
        doc.content.addAll(doc.proteomeComponents);

        doc.content.addAll(doc.xrefs);
        doc.content.addAll(doc.databases);

        // IMPORTANT: we also add information to content in other methods:
        //comments --> see convertComment method
        //features --> see convertFeature method
        //go Terms --> see convertGoTerms method
    }

    boolean isCanonicalIsoform(UniProtEntry uniProtEntry) {
        return uniProtEntry.getCommentByType(CommentType.ALTERNATIVE_PRODUCTS)
                .stream()
                .map(comment -> (AlternativeProductsComment) comment)
                .flatMap(comment -> comment.getIsoforms().stream())
                .filter(isoform -> isoform.getIsoformSequenceStatus() == IsoformSequenceStatus.DISPLAYED)
                .flatMap(isoform -> isoform.getIsoformIds().stream())
                .filter(isoformId -> isoformId.getValue().equals(uniProtEntry.getPrimaryAccession().getValue()))
                .count() == 1L;
    }

    private void setAvroDefaultEntry(UniProtEntry source, UniProtDocument doc) {
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

            byte[] avroByteArray = UniprotJsonConfig.getInstance().getObjectMapper().writeValueAsBytes(defaultObject);
            // can only store if it's size is small enough:
            // https://lucene.apache.org/core/7_5_0/core/org/apache/lucene/index/DocValuesType.html
            doc.avro_binary = getDefaultBinaryValue(Base64.getEncoder().encodeToString(avroByteArray));
        } catch (JsonProcessingException exception) {
            String accession = source.getPrimaryAccession().getValue();
            log.warn("Error saving default uniprot object in avro_binary for accession: " + accession, exception);
        }
    }

    private void setScore(UniProtEntry source, UniProtDocument doc) {
        UniProtEntryScored entryScored = new UniProtEntryScored(source);
        double score = entryScored.score();
        int q = (int) (score / 20d);
        doc.score = q > 4 ? 5 : q + 1;
    }

    private void setSequence(UniProtEntry source, UniProtDocument doc) {
        Sequence seq = source.getSequence();
        doc.seqLength = seq.getLength();
        doc.seqMass = seq.getMolWeight();
    }

    private void setProteinNames(UniProtEntry source, UniProtDocument doc) {
        if (source.hasProteinDescription()) {
            ProteinDescription proteinDescription = source.getProteinDescription();
            List<String> names = extractProteinDescriptionValues(proteinDescription);
            doc.proteinNames.addAll(names);
            doc.proteinsNamesSort = truncatedSortValue(String.join(" ", names));
        }
    }

    private void setECNumbers(UniProtEntry source, UniProtDocument doc) {
        if (source.hasProteinDescription()) {
            ProteinDescription proteinDescription = source.getProteinDescription();
            doc.ecNumbers = extractProteinDescriptionEcs(proteinDescription);
            doc.ecNumbersExact = doc.ecNumbers;
        }
    }

    private void setKeywords(UniProtEntry source, UniProtDocument doc) {
        if (source.hasKeywords()) {
            source.getKeywords().forEach(keyword -> updateKeyword(keyword, doc));
        }
    }

    private void updateKeyword(Keyword keyword, UniProtDocument doc) {
        doc.keywordIds.add(keyword.getId());
        doc.keywords.add(keyword.getId());
        addValueToStringList(doc.keywords, keyword);
        keywordRepo.getKeyword(keyword.getId()).ifPresent(kw -> {
            KeywordDetail category = kw.getCategory();
            if (category != null && !doc.keywordIds.contains(category.getKeyword().getAccession())) {
                doc.keywordIds.add(category.getKeyword().getAccession());
                doc.keywords.add(category.getKeyword().getAccession());
                doc.keywords.add(category.getKeyword().getId());
            }
        });
    }

    private void setOrganism(UniProtEntry source, UniProtDocument doc) {
        if (source.hasOrganism()) {
            int taxonomyId = Math.toIntExact(source.getOrganism().getTaxonId());
            doc.organismTaxId = taxonomyId;

            taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId).ifPresent(
                    node -> {
                        List<String> extractedTaxoNode = extractTaxonode(node);
                        doc.organismName.addAll(extractedTaxoNode);
                        doc.organismSort = truncatedSortValue(String.join(" ", extractedTaxoNode));

                        String popularOrganism = popularOrganismsTaxName.get(taxonomyId);
                        if (popularOrganism != null) {
                            doc.popularOrganism = popularOrganism;
                        } else {
                            if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
                                doc.otherOrganism = node.mnemonic();
                            } else if (node.commonName() != null && !node.commonName().isEmpty()) {
                                doc.otherOrganism = node.commonName();
                            } else {
                                doc.otherOrganism = node.scientificName();
                            }
                        }
                    }
            );
        }
    }

    private void setLineageTaxons(UniProtEntry source, UniProtDocument doc) {
        if (source.hasOrganism()) {
            int taxId = Math.toIntExact(source.getOrganism().getTaxonId());
            setLineageTaxon(taxId, doc);
        }
    }

    private void setOrganismHosts(UniProtEntry source, UniProtDocument doc) {
        List<OrganismHost> hosts = source.getOrganismHosts();
        doc.organismHostIds = extractOrganismHostsIds(hosts);
        doc.organismHostNames = extractOrganismHostsNames(hosts);
    }

    private void convertGeneNames(UniProtEntry source, UniProtDocument doc) {
        if (source.hasGenes()) {
            for (Gene gene : source.getGenes()) {
                addValueToStringList(doc.geneNamesExact, gene.getGeneName());
                addValueListToStringList(doc.geneNamesExact, gene.getSynonyms());
                addValueListToStringList(doc.geneNamesExact, gene.getOrderedLocusNames());
                addValueListToStringList(doc.geneNamesExact, gene.getOrfNames());
            }
        }

        if (!doc.geneNamesExact.isEmpty()) {
            doc.geneNames.addAll(doc.geneNamesExact);
            doc.geneNamesSort = truncatedSortValue(String.join(" ", doc.geneNames));
        }
    }

    private void convertGoTerms(UniProtEntry source, UniProtDocument doc) {
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
                addGOterm(evType, go.getId(), goTerm, doc);
                addGOParents(evType, go.getId(), doc);
                addPartOf(evType, go.getId(), doc);
                doc.content.add(go.getId().substring(3));// id
                doc.content.add(goTerm); // term
            }
        }
    }

    private void addGOParents(String evType, String goId, UniProtDocument doc) {
        goRelationRepo.getIsA(goId).forEach(term -> processGOterm(evType, term, doc));
    }

    private void addPartOf(String evType, String goId, UniProtDocument doc) {
        goRelationRepo.getPartOf(goId).forEach(term -> processGOterm(evType, term, doc));
    }

    private void processGOterm(String evType, GoTerm term, UniProtDocument doc) {
        addGOterm(evType, term.getId(), term.getName(), doc);
        addGOParents(evType, term.getId(), doc);
        addPartOf(evType, term.getId(), doc);
    }

    private void addGOterm(String evType, String goId, String term, UniProtDocument doc) {
        String key = GO + evType;
        Collection<String> values = doc.goWithEvidenceMaps.get(key);
        if (values == null) {
            values = new HashSet<>();
            doc.goWithEvidenceMaps.put(key, values);
        }
        values.add(goId.substring(3));
        values.add(term);

        doc.goes.add(goId.substring(3));
        doc.goes.add(term);
        doc.goIds.add(goId.substring(3));
    }

    private void convertRcs(List<ReferenceComment> referenceComments, UniProtDocument doc) {
        referenceComments.forEach(referenceComment -> {
            if (referenceComment.hasValue()) {
                switch (referenceComment.getType()) {
                    case STRAIN:
                        doc.rcStrain.add(referenceComment.getValue());
                        break;
                    case TISSUE:
                        doc.rcTissue.add(referenceComment.getValue());
                        break;
                    case PLASMID:
                        doc.rcPlasmid.add(referenceComment.getValue());
                        break;
                    case TRANSPOSON:
                        doc.rcTransposon.add(referenceComment.getValue());
                        break;
                }
            }
        });
    }

    private void convertRp(UniProtReference uniProtReference, UniProtDocument doc) {
        if (uniProtReference.hasReferencePositions()) {
            doc.scopes.addAll(uniProtReference.getReferencePositions());
        }
    }

    private void convertReferences(UniProtEntry source, UniProtDocument doc) {
        for (UniProtReference reference : source.getReferences()) {
            Citation citation = reference.getCitation();
            if (reference.hasReferenceComments()) {
                convertRcs(reference.getReferenceComments(), doc);
            }
            convertRp(reference, doc);
            if (citation.hasTitle()) {
                doc.referenceTitles.add(citation.getTitle());
            }
            if (citation.hasAuthors()) {
                doc.referenceAuthors.add(raLineBuilder.buildLine(citation.getAuthors(), false, false).get(0));
            }
            if (citation.hasAuthoringGroup()) {
                doc.referenceOrganizations
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
                        doc.referenceDates.add(dateFormatter.convertStringToDate(pubDate));
                    }
                } catch (ParseException e) {
                    log.warn("There was a problem converting entry dates during indexing:", e);
                }
            }

            citation.getCitationXrefsByType(CitationXrefType.PUBMED)
                    .ifPresent(pubmed -> doc.referencePubmeds.add(pubmed.getId()));
            if (citation instanceof JournalArticle) {
                JournalArticle ja = (JournalArticle) citation;
                doc.referenceJournals.add(ja.getJournal().getName());
            }
        }
    }

    private void convertComment(UniProtEntry source, UniProtDocument doc) {
        for (Comment comment : source.getComments()) {
            if (comment.getCommentType() == CommentType.COFACTOR) {
                convertFactor((CofactorComment) comment, doc);

            }
            if (comment.getCommentType() == CommentType.BIOPHYSICOCHEMICAL_PROPERTIES) {
                convertCommentBPCP((BPCPComment) comment, doc);

            }
            if (comment.getCommentType() == CommentType.SUBCELLULAR_LOCATION) {
                convertCommentSL((SubcellularLocationComment) comment, doc);

            }
            if (comment.getCommentType() == CommentType.ALTERNATIVE_PRODUCTS) {
                convertCommentAP((AlternativeProductsComment) comment, doc);

            }
            if (comment.getCommentType() == CommentType.SEQUENCE_CAUTION) {
                convertCommentSC((SequenceCautionComment) comment, doc);
            }
            if (comment.getCommentType() == CommentType.INTERACTION) {
                convertCommentInteraction((InteractionComment) comment, doc);
            }

            if (comment.getCommentType() == CommentType.SIMILARITY) {
                convertCommentFamily((FreeTextComment) comment, doc);
            }

            if (comment.getCommentType() == CommentType.PATHWAY) {
                convertPathway((FreeTextComment) comment, doc);
            }

            FFLineBuilder<Comment> fbuilder = CCLineBuilderFactory.create(comment);
            String field = getCommentField(comment);
            String evField = getCommentEvField(comment);
            Collection<String> value = doc.commentMap.computeIfAbsent(field, k -> new ArrayList<>());


            //@lgonzales doubt: can we change to without evidence??? because we already have the evidence map
            String commentVal = fbuilder.buildStringWithEvidence(comment);
            value.add(commentVal);
            doc.content.add(commentVal);

            Collection<String> evValue = doc.commentEvMap.computeIfAbsent(evField, k -> new HashSet<>());
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
                if (diseaseComment.hasDefinedDisease()) {
                    evidences.addAll(extractEvidence(
                            diseaseComment.getDisease().getEvidences()));
                    if (diseaseComment.hasNote() && diseaseComment.getNote().hasTexts()) {
                        evidences.addAll(extractEvidence(diseaseComment.getNote().getTexts().stream()
                                                                 .flatMap(val -> val.getEvidences().stream())
                                                                 .collect(Collectors.toList())));
                    }
                }
                break;
            case RNA_EDITING:
                RnaEditingComment reComment = (RnaEditingComment) comment;
                if (reComment.hasPositions()) {
                    evidences.addAll(extractEvidence(reComment.getPositions().stream()
                                                             .flatMap(val -> val.getEvidences().stream())
                                                             .collect(Collectors.toList())));
                }
                if (reComment.hasNote()) {
                    evidences.addAll(extractEvidence(reComment.getNote().getTexts().stream()
                                                             .flatMap(val -> val.getEvidences().stream())
                                                             .collect(Collectors.toList())));
                }
                break;
            case MASS_SPECTROMETRY:
                MassSpectrometryComment msComment = (MassSpectrometryComment) comment;
                evidences.addAll(extractEvidence(msComment.getEvidences()));
                break;
            default:
                break;
        }
        return evidences;
    }

    private void convertCommentInteraction(InteractionComment comment, UniProtDocument doc) {
        comment.getInteractions().forEach(interaction -> {
            if (interaction.hasFirstInteractor()) {
                doc.interactors.add(interaction.getFirstInteractor().getValue());
            }
            if (interaction.hasSecondInteractor()) {
                doc.interactors.add(interaction.getSecondInteractor().getValue());
            }
            if (interaction.hasUniProtAccession()) {
                doc.interactors.add(interaction.getUniProtAccession().getValue());
            }
        });
    }

    private void convertCommentFamily(FreeTextComment comment, UniProtDocument doc) {
        comment.getTexts().stream()
                .map(Value::getValue)
                .forEach(val -> updateFamily(val, doc));
    }

    private void convertPathway(FreeTextComment comment, UniProtDocument uniProtDocument) {
        comment.getTexts().stream()
                .map(Value::getValue)
                .forEach(val -> updatePathway(val, uniProtDocument));
    }

    private void updatePathway(String val, UniProtDocument uniProtDocument) {
        UniPathway unipathway = pathwayRepo.getFromName(val);
        if (unipathway != null) {
            uniProtDocument.pathway.add(unipathway.getAccession());
        }
    }

    private void updateFamily(String val, UniProtDocument uniProtDocument) {
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
            uniProtDocument.familyInfo.add(line.toString());
        }
    }

    private void convertCommentSC(SequenceCautionComment comment, UniProtDocument uniProtDocument) {
        String val = comment.getSequenceCautionType().toDisplayName();
        if (comment.hasNote()) {
            val = comment.getNote();
        }
        Set<String> evidence = extractEvidence(comment.getEvidences());
        uniProtDocument.seqCaution.add(val);
        uniProtDocument.seqCautionEv.addAll(evidence);
        switch (comment.getSequenceCautionType()) {
            case FRAMESHIFT:
                uniProtDocument.seqCautionFrameshift.add(val);
                break;
            case ERRONEOUS_INITIATION:
                uniProtDocument.seqCautionErInit.add(val);
                break;
            case ERRONEOUS_TERMIINATION:
                uniProtDocument.seqCautionErTerm.add(val);
                break;
            case ERRONEOUS_PREDICTION:
                uniProtDocument.seqCautionErPred.add(val);
                break;
            case ERRONEOUS_TRANSLATION:
                uniProtDocument.seqCautionErTran.add(val);
                break;
            case MISCELLANEOUS_DISCREPANCY:
                uniProtDocument.seqCautionMisc.add(val);
                uniProtDocument.seqCautionMiscEv.addAll(evidence);
                break;
            default:
                break;
        }
    }

    private void convertCommentBPCP(BPCPComment comment, UniProtDocument doc) {
        if (comment.hasAbsorption()) {
            Absorption absorption = comment.getAbsorption();
            doc.bpcpAbsorption.add("" + absorption.getMax());
            doc.bpcpAbsorptionEv.addAll(extractEvidence(absorption.getEvidences()));
            if (absorption.getNote() != null) {
                doc.bpcpAbsorption.addAll(absorption.getNote().getTexts().stream().map(Value::getValue)
                                                  .collect(Collectors.toList()));
                doc.bpcpAbsorptionEv.addAll(extractEvidence(absorption.getNote().getTexts().stream()
                                                                    .flatMap(val -> val.getEvidences()
                                                                            .stream())
                                                                    .collect(Collectors.toList())));
            }
            doc.bpcp.addAll(doc.bpcpAbsorption);
            doc.bpcpEv.addAll(doc.bpcpAbsorptionEv);
        }
        if (comment.hasKineticParameters()) {
            KineticParameters kp = comment.getKineticParameters();
            kp.getMaximumVelocities().stream().map(MaximumVelocity::getEnzyme)
                    .filter(val -> !Strings.isNullOrEmpty(val)).forEach(doc.bpcpKinetics::add);
            doc.bpcpKineticsEv.addAll(extractEvidence(kp.getMaximumVelocities().stream()
                                                              .flatMap(val -> val.getEvidences().stream())
                                                              .collect(Collectors.toList())));
            kp.getMichaelisConstants().stream().map(MichaelisConstant::getSubstrate)
                    .filter(val -> !Strings.isNullOrEmpty(val)).forEach(doc.bpcpKinetics::add);
            doc.bpcpKineticsEv.addAll(extractEvidence(kp.getMichaelisConstants().stream()
                                                              .flatMap(val -> val.getEvidences().stream())
                                                              .collect(Collectors.toList())));
            if (kp.getNote() != null) {
                doc.bpcpKinetics.addAll(
                        kp.getNote().getTexts().stream().map(Value::getValue).collect(Collectors.toList()));
                doc.bpcpKineticsEv.addAll(extractEvidence(kp.getNote().getTexts().stream()
                                                                  .flatMap(val -> val.getEvidences()
                                                                          .stream())
                                                                  .collect(Collectors.toList())));
            }
            if (doc.bpcpKinetics.isEmpty()) {
                kp.getMaximumVelocities().stream().map(val -> "" + val.getVelocity())
                        .forEach(doc.bpcpKinetics::add);
                kp.getMichaelisConstants().stream().map(val -> "" + val.getConstant())
                        .forEach(doc.bpcpKinetics::add);
            }
            doc.bpcp.addAll(doc.bpcpKinetics);
            doc.bpcpEv.addAll(doc.bpcpKineticsEv);

        }
        if (comment.hasPhDependence()) {
            comment.getPhDependence().getTexts().stream().map(Value::getValue)
                    .forEach(doc.bpcpPhDependence::add);
            doc.bpcpPhDependenceEv.addAll(extractEvidence(comment.getPhDependence().getTexts().stream()
                                                                  .flatMap(val -> val.getEvidences()
                                                                          .stream())
                                                                  .collect(Collectors.toList())));
            doc.bpcp.addAll(doc.bpcpPhDependence);
            doc.bpcpEv.addAll(doc.bpcpPhDependenceEv);
        }
        if (comment.hasRedoxPotential()) {
            comment.getRedoxPotential().getTexts().stream().map(Value::getValue)
                    .forEach(doc.bpcpRedoxPotential::add);
            doc.bpcpRedoxPotentialEv.addAll(extractEvidence(comment.getRedoxPotential().getTexts().stream()
                                                                    .flatMap(val -> val.getEvidences()
                                                                            .stream())
                                                                    .collect(Collectors.toList())));
            doc.bpcp.addAll(doc.bpcpRedoxPotential);
            doc.bpcpEv.addAll(doc.bpcpRedoxPotentialEv);
        }
        if (comment.hasTemperatureDependence()) {
            comment.getTemperatureDependence().getTexts().stream().map(Value::getValue)
                    .forEach(doc.bpcpTempDependence::add);
            doc.bpcpTempDependenceEv.addAll(extractEvidence(comment.getTemperatureDependence().getTexts()
                                                                    .stream()
                                                                    .flatMap(val -> val.getEvidences()
                                                                            .stream())
                                                                    .collect(Collectors.toList())));
            doc.bpcp.addAll(doc.bpcpTempDependence);
            doc.bpcpEv.addAll(doc.bpcpTempDependenceEv);
        }
    }

    private void convertCommentSL(SubcellularLocationComment comment, UniProtDocument doc) {
        if (comment.hasSubcellularLocations()) {
            comment.getSubcellularLocations().forEach(subcellularLocation -> {
                if (subcellularLocation.hasLocation()) {
                    doc.subcellLocationTerm.add(subcellularLocation.getLocation().getValue());

                    Set<String> locationEv = extractEvidence(subcellularLocation.getLocation().getEvidences());
                    doc.subcellLocationTermEv.addAll(locationEv);

                }
                if (subcellularLocation.hasOrientation()) {
                    doc.subcellLocationTerm.add(subcellularLocation.getOrientation().getValue());

                    Set<String> orientationEv = extractEvidence(subcellularLocation.getOrientation().getEvidences());
                    doc.subcellLocationTermEv.addAll(orientationEv);
                }
                if (subcellularLocation.hasTopology()) {
                    doc.subcellLocationTerm.add(subcellularLocation.getTopology().getValue());

                    Set<String> topologyEv = extractEvidence(subcellularLocation.getTopology().getEvidences());
                    doc.subcellLocationTermEv.addAll(topologyEv);
                }
            });
        }
        if (comment.hasNote()) {
            comment.getNote().getTexts().stream().map(Value::getValue)
                    .forEach(doc.subcellLocationNote::add);
            Set<String> noteEv = extractEvidence(comment.getNote().getTexts().stream()
                                                         .flatMap(val -> val.getEvidences().stream())
                                                         .collect(Collectors.toList()));
            doc.subcellLocationNoteEv.addAll(noteEv);
        }
    }

    private Set<String> extractEvidence(List<Evidence> evidences) {
        Set<String> extracted = evidences.stream().map(ev -> ev.getEvidenceCode().name()).collect(Collectors.toSet());
        if (extracted.stream().anyMatch(MANUAL_EVIDENCE_MAP::contains)) {
            extracted.add(MANUAL_EVIDENCE);
        }
        if (extracted.stream().anyMatch(AUTOMATIC_EVIDENCE_MAP::contains)) {
            extracted.add(AUTOMATIC_EVIDENCE);
        }
        if (extracted.stream().anyMatch(EXPERIMENTAL_EVIDENCE_MAP::contains)) {
            extracted.add(EXPERIMENTAL_EVIDENCE);
        }
        return extracted;
    }

    private void convertCommentAP(AlternativeProductsComment comment, UniProtDocument doc) {
        List<String> values = new ArrayList<>();
        Set<String> evidence = new HashSet<>();
        if (comment.hasNote()) {
            comment.getNote().getTexts().stream().map(Value::getValue)
                    .forEach(values::add);

            evidence.addAll(extractEvidence(comment.getNote().getTexts().stream()
                                                    .flatMap(val -> val.getEvidences().stream())
                                                    .collect(Collectors.toList())));
        }

        List<String> events = new ArrayList<>();
        if (comment.hasEvents()) {
            comment.getEvents().stream().map(APEventType::getName).forEach(events::add);
            values.addAll(events);
        }

        doc.ap.addAll(values);
        doc.apEv.addAll(evidence);
        for (String event : events) {
            if ("alternative promoter usage".equalsIgnoreCase(event)) {
                doc.apApu.addAll(values);
                doc.apApuEv.addAll(evidence);
            }
            if ("alternative splicing".equalsIgnoreCase(event)) {
                doc.apAs.addAll(values);
                doc.apAsEv.addAll(evidence);
            }
            if ("alternative initiation".equalsIgnoreCase(event)) {
                doc.apAi.addAll(values);
                doc.apAiEv.addAll(evidence);
            }
            if ("ribosomal frameshifting".equalsIgnoreCase(event)) {
                doc.apRf.addAll(values);
                doc.apRfEv.addAll(evidence);
            }
        }

    }

    private void convertFactor(CofactorComment comment, UniProtDocument doc) {
        if (comment.hasCofactors()) {
            comment.getCofactors().forEach(val -> {
                doc.cofactorChebi.add(val.getName());
                if (val.getCofactorReference().getDatabaseType() == CofactorReferenceType.CHEBI) {
                    String id = val.getCofactorReference().getId();
                    if (id.startsWith("CHEBI:"))
                        id = id.substring("CHEBI:".length());
                    doc.cofactorChebi.add(id);
                }
                doc.cofactorChebiEv.addAll(extractEvidence(val.getEvidences()));
            });
        }

        if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
            comment.getNote().getTexts().forEach(val -> {
                doc.cofactorNote.add(val.getValue());
                doc.cofactorNoteEv.addAll(extractEvidence(val.getEvidences()));
            });
        }
    }

    private void convertXref(UniProtEntry source, UniProtDocument doc) {
        boolean d3structure = false;
        for (UniProtDBCrossReference xref : source.getDatabaseCrossReferences()) {
            if (xref.getDatabaseType().getName().equalsIgnoreCase("PDB")) {
                d3structure = true;
            }
            String dbname = xref.getDatabaseType().getName().toLowerCase();
            doc.databases.add(dbname);
            String id = xref.getId();
            if (!dbname.equalsIgnoreCase("PIR")) {
                addXrefId(id, dbname, doc.xrefs);
            }
            switch (dbname.toLowerCase()) {
                case "embl":
                    if (xref.hasProperties()) {
                        Optional<String> proteinId = xref.getProperties().stream()
                                .filter(property -> property.getKey().equalsIgnoreCase("ProteinId"))
                                .filter(property -> !property.getValue().equalsIgnoreCase("-"))
                                .map(Property::getValue)
                                .findFirst();
                        proteinId.ifPresent(s -> addXrefId(s, dbname, doc.xrefs));
                    }
                    break;
                case "refseq":
                case "pir":
                case "unipathway":
                case "ensembl":
                    if (xref.hasProperties()) {
                        List<String> properties = xref.getProperties().stream()
                                .filter(property -> !property.getValue().equalsIgnoreCase("-"))
                                .map(Property::getValue)
                                .collect(Collectors.toList());
                        properties.forEach(s -> addXrefId(s, dbname, doc.xrefs));
                    }
                    break;
                case "proteomes":
                    doc.proteomes.add(xref.getId());
                    if (xref.hasProperties()) {
                        doc.proteomeComponents.addAll(xref.getProperties().stream()
                                                              .map(Property::getValue)
                                                              .collect(Collectors.toSet()));
                    }
                default:
                    break;
            }
        }
        doc.d3structure = d3structure;
    }

    private void addXrefId(String id, String dbname, Collection<String> values) {
        values.add(id);
        values.add(dbname + "-" + id);
        if (id.indexOf(".") > 0) {
            String idMain = id.substring(0, id.indexOf('.'));
            values.add(idMain);
            values.add(dbname + "-" + idMain);
        }
    }

    private void setOrganelle(UniProtEntry source, UniProtDocument doc) {
        for (GeneLocation geneLocation : source.getGeneLocations()) {
            GeneEncodingType geneEncodingType = geneLocation.getGeneEncodingType();

            if (PLASTID_CHILD.contains(geneEncodingType)) {
                doc.organelles.add(GeneEncodingType.PLASTID.getName());
            }

            String organelleValue = geneEncodingType.getName();
            if (geneLocation.getValue() != null && !geneLocation.getValue().isEmpty()) {
                organelleValue += " " + geneLocation.getValue();
            }
            organelleValue = organelleValue.trim();

            doc.organelles.add(organelleValue);
        }
    }

    private void convertFeature(UniProtEntry source, UniProtDocument doc) {
        for (Feature feature : source.getFeatures()) {
            FFLineBuilder<Feature> fbuilder = FeatureLineBuilderFactory.create(feature);

            String field = getFeatureField(feature, FEATURE);
            String evField = getFeatureField(feature, FT_EV);
            String lengthField = getFeatureField(feature, FT_LENGTH);
            Collection<String> featuresOfTypeList = doc.featuresMap
                    .computeIfAbsent(field, k -> new HashSet<>());
            String featureText = fbuilder.buildStringWithEvidence(feature);
            featuresOfTypeList.add(featureText);
            doc.content.add(featureText);

            // start and end of location
            int length = feature.getLocation().getEnd().getValue() - feature.getLocation().getStart().getValue() + 1;
            Set<String> evidences = extractEvidence(feature.getEvidences());
            Collection<Integer> lengthList = doc.featureLengthMap
                    .computeIfAbsent(lengthField, k -> new HashSet<>());
            lengthList.add(length);

            Collection<String> evidenceList = doc.featureEvidenceMap
                    .computeIfAbsent(evField, k -> new HashSet<>());
            evidenceList.addAll(evidences);
        }
    }

    private void setFragmentNPrecursor(UniProtEntry source, UniProtDocument doc) {
        ProteinDescription description = source.getProteinDescription();
        boolean isFragment = false;
        boolean isPrecursor = false;
        if (source.hasProteinDescription() && description.hasFlag()) {
            Flag flag = description.getFlag();
            switch (flag.getType()) {
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
        doc.fragment = isFragment;
        doc.precursor = isPrecursor;
    }

    private void setProteinExistence(UniProtEntry source, UniProtDocument doc) {
        ProteinExistence proteinExistence = source.getProteinExistence();
        if (proteinExistence != null) {
            doc.proteinExistence = proteinExistence.name();
        }
    }

    private List<String> extractProteinDescriptionEcs(ProteinDescription proteinDescription) {
        List<String> ecs = new ArrayList<>();
        if (proteinDescription.hasRecommendedName() && proteinDescription.getRecommendedName().hasEcNumbers()) {
            ecs.addAll(getEcs(proteinDescription.getRecommendedName().getEcNumbers()));
        }
        if (proteinDescription.hasSubmissionNames()) {
            proteinDescription.getSubmissionNames().stream()
                    .filter(ProteinSubName::hasEcNumbers)
                    .flatMap(proteinSubName -> getEcs(proteinSubName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        if (proteinDescription.hasAlternativeNames()) {
            proteinDescription.getAlternativeNames().stream()
                    .filter(ProteinAltName::hasEcNumbers)
                    .flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        if (proteinDescription.hasContains()) {
            proteinDescription.getContains().stream()
                    .flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream())
                    .forEach(ecs::add);
        }
        if (proteinDescription.hasIncludes()) {
            proteinDescription.getIncludes().stream()
                    .flatMap(proteinSection -> getProteinSectionEcs(proteinSection).stream())
                    .forEach(ecs::add);
        }
        return ecs;
    }

    private List<String> getProteinSectionEcs(ProteinSection proteinSection) {
        List<String> ecs = new ArrayList<>();
        if (proteinSection.hasRecommendedName() && proteinSection.getRecommendedName().hasEcNumbers()) {
            ecs.addAll(getEcs(proteinSection.getRecommendedName().getEcNumbers()));
        }
        if (proteinSection.hasAlternativeNames()) {
            proteinSection.getAlternativeNames().stream()
                    .filter(ProteinAltName::hasEcNumbers)
                    .flatMap(proteinAltName -> getEcs(proteinAltName.getEcNumbers()).stream())
                    .forEach(ecs::add);
        }
        return ecs;
    }

    private List<String> getEcs(List<EC> ecs) {
        return ecs.stream()
                .map(EC::getValue)
                .collect(Collectors.toList());
    }

    private List<String> extractProteinDescriptionValues(ProteinDescription description) {
        List<String> values = new ArrayList<>();
        if (description.hasRecommendedName()) {
            values.addAll(getProteinRecNameNames(description.getRecommendedName()));
        }
        if (description.hasSubmissionNames()) {
            description.getSubmissionNames().stream()
                    .map(this::getProteinSubNameNames)
                    .forEach(values::addAll);
        }
        if (description.hasAlternativeNames()) {
            description.getAlternativeNames().stream()
                    .map(this::getProteinAltNameNames)
                    .forEach(values::addAll);
        }
        if (description.hasContains()) {
            description.getContains().stream()
                    .map(this::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if (description.hasIncludes()) {
            description.getIncludes().stream()
                    .map(this::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if (description.hasAllergenName()) {
            values.add(description.getAllergenName().getValue());
        }
        if (description.hasBiotechName()) {
            values.add(description.getBiotechName().getValue());
        }
        if (description.hasCdAntigenNames()) {
            description.getCdAntigenNames().stream()
                    .map(Value::getValue)
                    .forEach(values::add);
        }
        if (description.hasInnNames()) {
            description.getInnNames().stream()
                    .map(Value::getValue)
                    .forEach(values::add);
        }
        return values;
    }

    private List<String> getProteinSectionValues(ProteinSection proteinSection) {
        List<String> names = new ArrayList<>();
        if (proteinSection.hasRecommendedName()) {
            names.addAll(getProteinRecNameNames(proteinSection.getRecommendedName()));
        }
        if (proteinSection.hasAlternativeNames()) {
            proteinSection.getAlternativeNames().stream()
                    .map(this::getProteinAltNameNames)
                    .forEach(names::addAll);
        }
        if (proteinSection.hasCdAntigenNames()) {
            proteinSection.getCdAntigenNames().stream()
                    .map(Value::getValue)
                    .forEach(names::add);
        }
        if (proteinSection.hasAllergenName()) {
            names.add(proteinSection.getAllergenName().getValue());
        }
        if (proteinSection.hasInnNames()) {
            proteinSection.getInnNames().stream()
                    .map(Value::getValue)
                    .forEach(names::add);
        }
        if (proteinSection.hasBiotechName()) {
            names.add(proteinSection.getBiotechName().getValue());
        }
        return names;
    }

    private List<String> getProteinRecNameNames(ProteinRecName proteinRecName) {
        List<String> names = new ArrayList<>();
        if (proteinRecName.hasFullName()) {
            names.add(proteinRecName.getFullName().getValue());
        }
        if (proteinRecName.hasShortNames()) {
            proteinRecName.getShortNames()
                    .stream()
                    .map(Name::getValue)
                    .forEach(names::add);
        }
        return names;
    }

    private List<String> getProteinAltNameNames(ProteinAltName proteinAltName) {
        List<String> names = new ArrayList<>();
        if (proteinAltName.hasShortNames()) {
            proteinAltName.getShortNames()
                    .stream()
                    .map(Name::getValue)
                    .forEach(names::add);
        }
        if (proteinAltName.hasFullName()) {
            names.add(proteinAltName.getFullName().getValue());
        }
        return names;
    }

    private List<String> getProteinSubNameNames(ProteinSubName proteinAltName) {
        List<String> names = new ArrayList<>();
        if (proteinAltName.hasFullName()) {
            names.add(proteinAltName.getFullName().getValue());
        }
        return names;
    }

    private void setLineageTaxon(int taxId, UniProtDocument doc) {
        if (taxId > 0) {
            Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);

            while (taxonomicNode.isPresent()) {
                TaxonomicNode node = taxonomicNode.get();
                doc.taxLineageIds.add(node.id());
                doc.organismTaxon.addAll(extractTaxonode(node));
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
