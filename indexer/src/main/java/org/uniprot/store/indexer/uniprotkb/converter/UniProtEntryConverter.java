package org.uniprot.store.indexer.uniprotkb.converter;

import static org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Sequence;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.scorer.uniprotkb.UniProtEntryScored;
import org.uniprot.core.uniprotkb.*;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceDatabase;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.chebi.ChebiRepo;
import org.uniprot.cv.ec.ECRepo;
import org.uniprot.cv.go.GORepo;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.uniprot.pathway.PathwayRepo;
import org.uniprot.store.indexer.util.DateUtils;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import lombok.extern.slf4j.Slf4j;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryConverter implements DocumentConverter<UniProtKBEntry, UniProtDocument> {

    private static final String DASH = "-";
    private static final String CANONICAL = DASH + "1";
    static final int SPELLCHECK_MIN_LENGTH = 4;
    /** An enum set representing all of the organelles(encoded in) that are children of plastid */
    private static final EnumSet<GeneEncodingType> PLASTID_CHILD =
            EnumSet.of(
                    GeneEncodingType.APICOPLAST,
                    GeneEncodingType.CHLOROPLAST,
                    GeneEncodingType.CYANELLE,
                    GeneEncodingType.NON_PHOTOSYNTHETIC_PLASTID,
                    GeneEncodingType.ORGANELLAR_CHROMATOPHORE);

    private final UniProtEntryCommentsConverter commentsConverter;
    private final UniProtEntryFeatureConverter featureConverter;
    private final UniProtEntryReferencesConverter referencesConverter;
    private final UniProtEntryCrossReferenceConverter crossReferenceConverter;
    private final UniProtEntryTaxonomyConverter taxonomyConverter;
    private final UniprotKBEntryProteinDescriptionConverter proteinDescriptionConverter;

    private Map<String, SuggestDocument> suggestions;

    public UniProtEntryConverter(
            TaxonomyRepo taxonomyRepo,
            GORepo goRepo,
            PathwayRepo pathwayRepo,
            ChebiRepo chebiRepo,
            ECRepo ecRepo,
            Map<String, SuggestDocument> suggestDocuments) {
        this.taxonomyConverter = new UniProtEntryTaxonomyConverter(taxonomyRepo, suggestDocuments);
        this.crossReferenceConverter =
                new UniProtEntryCrossReferenceConverter(goRepo, suggestDocuments);
        this.commentsConverter =
                new UniProtEntryCommentsConverter(chebiRepo, pathwayRepo, suggestDocuments);
        this.featureConverter = new UniProtEntryFeatureConverter(chebiRepo, suggestDocuments);
        this.referencesConverter = new UniProtEntryReferencesConverter();
        this.proteinDescriptionConverter =
                new UniprotKBEntryProteinDescriptionConverter(ecRepo, suggestDocuments);
        this.suggestions = suggestDocuments;
    }

    @Override
    public UniProtDocument convert(UniProtKBEntry source) {
        try {
            UniProtDocument document = new UniProtDocument();

            document.accession = source.getPrimaryAccession().getValue();
            if (document.accession.contains(DASH)) {
                if (isCanonicalIsoform(source)) {
                    document.reviewed = null;
                    document.isIsoform = null;
                    return document;
                }
                document.isIsoform = true;
                // We are adding the canonical accession to the isoform entry as a secondary
                // accession.
                // this way when you search by an accession and ask to include isoforms, it will
                // find it.
                String canonicalAccession =
                        document.accession.substring(0, document.accession.indexOf(DASH));
                document.canonicalAccession = canonicalAccession;
            } else {
                document.isIsoform = false;
                if (!hasIsoform(source)) {
                    document.canonicalAccession = document.accession + CANONICAL;
                }
            }
            document.reviewed = (source.getEntryType() == UniProtKBEntryType.SWISSPROT);
            addValueListToStringList(document.secacc, source.getSecondaryAccessions());

            proteinDescriptionConverter.convertProteinDescription(
                    source.getProteinDescription(), document);
            taxonomyConverter.convertOrganism(source.getOrganism(), document);
            taxonomyConverter.convertOrganismHosts(source.getOrganismHosts(), document);
            referencesConverter.convertReferences(source.getReferences(), document);
            commentsConverter.convertCommentToDocument(source.getComments(), document);
            crossReferenceConverter.convertCrossReferences(
                    source.getUniProtKBCrossReferences(), document);
            featureConverter.convertFeature(source.getFeatures(), document);
            convertUniprotId(source.getUniProtkbId(), document);
            convertEntryAudit(source.getEntryAudit(), document);
            convertGeneNames(source.getGenes(), document);
            convertKeywords(source.getKeywords(), document);
            convertEncodedIn(source.getGeneLocations(), document);
            convertProteinExistence(source.getProteinExistence(), document);
            convertSequence(source.getSequence(), document);
            convertEntryScore(source, document);
            convertEvidenceSources(source, document);
            populateSpellcheckSuggestions(document);
            return document;
        } catch (Exception e) {
            String message = "Error converting UniProt entry";
            if (source != null && source.getPrimaryAccession() != null) {
                message += ": " + source.getPrimaryAccession().getValue();
            }
            log.error(message, e);
            throw new DocumentConversionException(message, e);
        }
    }

    private boolean hasIsoform(UniProtKBEntry source) {
        return source.hasComments()
                && !source.getCommentsByType(CommentType.ALTERNATIVE_PRODUCTS).isEmpty();
    }

    private void convertEntryAudit(EntryAudit entryAudit, UniProtDocument document) {
        if (entryAudit != null) {
            document.firstCreated =
                    DateUtils.convertLocalDateToUTCDate(entryAudit.getFirstPublicDate());
            document.lastModified =
                    DateUtils.convertLocalDateToUTCDate(entryAudit.getLastAnnotationUpdateDate());
            document.sequenceUpdated =
                    DateUtils.convertLocalDateToUTCDate(entryAudit.getLastSequenceUpdateDate());
        }
    }

    private void convertEvidenceSources(UniProtKBEntry uniProtkbEntry, UniProtDocument document) {
        List<Evidence> evidences = uniProtkbEntry.gatherEvidences();
        document.sources =
                evidences.stream()
                        .map(Evidence::getEvidenceCrossReference)
                        .filter(Objects::nonNull)
                        .flatMap(this::getSourceValues)
                        .filter(Objects::nonNull)
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet());
    }

    private void convertEntryScore(UniProtKBEntry source, UniProtDocument document) {
        UniProtEntryScored entryScored = new UniProtEntryScored(source);
        double score = entryScored.score();
        int q = (int) (score / 20d);
        if (!source.hasAnnotationScore()) {
            document.score = q > 4 ? 5 : q + 1;
        } else {
            document.score = (int) source.getAnnotationScore();
        }
    }

    private void convertUniprotId(UniProtKBId uniProtkbId, UniProtDocument document) {
        document.id.add(uniProtkbId.getValue());
        String[] idParts = uniProtkbId.getValue().split("_");
        if (idParts.length == 2) {
            if (document.reviewed) {
                // first component of swiss-prot id is gene, which we want searchable in the
                // mnemonic
                document.idDefault.addAll(document.id);
            } else {
                // don't add first component for trembl entries, since this is the accession,
                // and
                // we do not want false boosting for default searches that match a substring of
                // the accession
                document.idDefault.add(idParts[1]);
                document.content.add(uniProtkbId.getValue());
            }
        }
    }

    private Stream<String> getSourceValues(CrossReference<EvidenceDatabase> xref) {
        List<String> sources = new ArrayList<>();
        if (xref.hasId()) {
            sources.add(xref.getId());
        }
        if (xref.hasDatabase()) {
            if (xref.getDatabase().getName().equalsIgnoreCase("HAMAP-rule")) {
                sources.add("HAMAP");
            } else {
                sources.add(xref.getDatabase().getName());
            }
        }
        return sources.stream();
    }

    private void convertSequence(Sequence seq, UniProtDocument document) {
        document.seqLength = seq.getLength();
        document.seqMass = seq.getMolWeight();
    }

    private void convertKeywords(List<Keyword> keywords, UniProtDocument document) {
        if (Utils.notNullNotEmpty(keywords)) {
            keywords.forEach(keyword -> updateKeyword(keyword, document));
        }
    }

    private void updateKeyword(Keyword keyword, UniProtDocument document) {
        document.keywords.add(keyword.getId());
        document.keywords.add(keyword.getName());
        KeywordCategory kc = keyword.getCategory();
        if (!document.keywords.contains(kc.getId())) {
            document.keywords.add(kc.getId());
            document.keywords.add(kc.getName());
        }

        suggestions.putIfAbsent(
                createSuggestionMapKey(SuggestDictionary.KEYWORD, keyword.getId()),
                SuggestDocument.builder()
                        .id(keyword.getId())
                        .value(keyword.getName())
                        .dictionary(SuggestDictionary.KEYWORD.name())
                        .build());

        suggestions.putIfAbsent(
                createSuggestionMapKey(SuggestDictionary.KEYWORD, kc.getId()),
                SuggestDocument.builder()
                        .id(kc.getId())
                        .value(kc.getName())
                        .dictionary(SuggestDictionary.KEYWORD.name())
                        .build());
    }

    private void convertGeneNames(List<Gene> genes, UniProtDocument document) {
        if (Utils.notNullNotEmpty(genes)) {
            for (Gene gene : genes) {
                addValueToStringList(document.geneNamesExact, gene.getGeneName());
                addValueListToStringList(document.geneNamesExact, gene.getSynonyms());
                addValueListToStringList(document.geneNamesExact, gene.getOrderedLocusNames());
                addValueListToStringList(document.geneNamesExact, gene.getOrfNames());
            }
            document.geneNames.addAll(document.geneNamesExact);
            document.geneNamesSort = truncatedSortValue(String.join(" ", document.geneNames));
        }
    }

    private void convertEncodedIn(List<GeneLocation> geneLocations, UniProtDocument document) {
        if (Utils.notNullNotEmpty(geneLocations)) {
            for (GeneLocation geneLocation : geneLocations) {
                GeneEncodingType geneEncodingType = geneLocation.getGeneEncodingType();

                if (PLASTID_CHILD.contains(geneEncodingType)) {
                    document.encodedIn.add(GeneEncodingType.PLASTID.getName().toLowerCase());
                }
                document.encodedIn.add(geneEncodingType.getName().toLowerCase());
            }
        }
    }

    private void convertProteinExistence(
            ProteinExistence proteinExistence, UniProtDocument document) {
        if (proteinExistence != null) {
            document.proteinExistence = proteinExistence.getId();
        }
    }

    public static void populateSpellcheckSuggestions(UniProtDocument document) {
        // populate fields used in spellcheck
        Collection<String> diseases = document.commentMap.getOrDefault("cc_disease", List.of());
        populateSuggestions(diseases, document);
        populateSuggestions(document.proteinNames, document);
        populateSuggestions(document.geneNamesExact, document);
        populateSuggestions(document.organismTaxon, document);
        populateSuggestions(document.rcStrain, document);
    }

    private static void populateSuggestions(Collection<String> values, UniProtDocument document) {
        Set<String> length4orMore =
                values.stream()
                        .filter(val -> val.length() >= SPELLCHECK_MIN_LENGTH)
                        .collect(Collectors.toSet());
        document.suggests.addAll(length4orMore);
    }

    public Object getSuggestions() {
        return this.suggestions;
    }
}
