package org.uniprot.store.indexer.uniprotkb.converter;

import static org.uniprot.core.util.Utils.notNull;
import static org.uniprot.core.util.Utils.nullOrEmpty;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.uniprot.core.CrossReference;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.uniprotkb.feature.Ligand;
import org.uniprot.core.uniprotkb.feature.LigandPart;
import org.uniprot.core.uniprotkb.feature.UniProtKBFeature;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureDatabase;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.chebi.ChebiRepo;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.ProteinsWith;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
class UniProtEntryFeatureConverter {

    private static final String CHEBI2 = "CHEBI:";
    private static final String FEATURE = "ft_";
    private static final String FT_LENGTH = "ftlen_";
    private final ChebiRepo chebiRepo;
    private final Map<String, SuggestDocument> suggestions;

    public UniProtEntryFeatureConverter(
            ChebiRepo chebiRepo, Map<String, SuggestDocument> suggestDocuments) {
        this.chebiRepo = chebiRepo;
        this.suggestions = suggestDocuments;
    }

    void convertFeature(List<UniProtKBFeature> features, UniProtDocument document) {
        for (UniProtKBFeature feature : features) {
            String field = getFeatureField(feature, FEATURE);
            String lengthField = getFeatureField(feature, FT_LENGTH);
            Collection<String> featuresOfTypeList =
                    document.featuresMap.computeIfAbsent(field, k -> new HashSet<>());

            featuresOfTypeList.add(feature.getType().getName());
            document.content.add(feature.getType().getName());

            if (feature.hasFeatureId()) {
                featuresOfTypeList.add(feature.getFeatureId().getValue());
                document.content.add(feature.getFeatureId().getValue());
            }

            ProteinsWith.from(feature.getType())
                    .map(ProteinsWith::getValue)
                    .filter(value -> !document.proteinsWith.contains(value)) // avoid duplicated
                    .ifPresent(value -> document.proteinsWith.add(value));

            if (feature.hasDescription()) {
                featuresOfTypeList.add(feature.getDescription().getValue());
                document.content.add(feature.getDescription().getValue());
            }

            // start and end of location
            int length =
                    feature.getLocation().getEnd().getValue()
                            - feature.getLocation().getStart().getValue()
                            + 1;
            Set<String> evidences =
                    UniProtEntryConverterUtil.extractEvidence(feature.getEvidences());
            addFeatureCrossReferences(feature, document, featuresOfTypeList);
            if (feature.hasLigand()) {
                Ligand ligand = feature.getLigand();
                featuresOfTypeList.add(ligand.getName());
                document.content.add(ligand.getName());
                if (Utils.notNullNotEmpty(ligand.getNote())) {
                    featuresOfTypeList.add(ligand.getNote());
                    document.content.add(ligand.getNote());
                }
                if (Utils.notNullNotEmpty(ligand.getLabel())) {
                    featuresOfTypeList.add(ligand.getLabel());
                    document.content.add(ligand.getLabel());
                }
            }

            if (feature.hasLigandPart()) {
                LigandPart ligandPart = feature.getLigandPart();
                featuresOfTypeList.add(ligandPart.getName());
                document.content.add(ligandPart.getName());
                if (Utils.notNullNotEmpty(ligandPart.getNote())) {
                    featuresOfTypeList.add(ligandPart.getNote());
                    document.content.add(ligandPart.getNote());
                }
                if (Utils.notNullNotEmpty(ligandPart.getLabel())) {
                    featuresOfTypeList.add(ligandPart.getLabel());
                    document.content.add(ligandPart.getLabel());
                }
            }
        }
    }

    private void addFeatureCrossReferences(
            UniProtKBFeature feature,
            UniProtDocument document,
            Collection<String> featuresOfTypeList) {
        if (Utils.notNullNotEmpty(feature.getFeatureCrossReferences())) {
            feature.getFeatureCrossReferences()
                    .forEach(
                            xref -> {
                                addFeatureCrossReference(xref, document, featuresOfTypeList);
                            });
        }
    }

    private void addFeatureCrossReference(
            CrossReference<UniprotKBFeatureDatabase> xref,
            UniProtDocument document,
            Collection<String> featuresOfTypeList) {
        String xrefId = xref.getId();
        UniprotKBFeatureDatabase dbType = xref.getDatabase();
        String dbname = dbType.getName();

        List<String> xrefIds = UniProtEntryConverterUtil.getXrefId(xrefId, dbname);
        document.crossRefs.addAll(xrefIds);
        document.content.addAll(xrefIds);
        document.databases.add(dbname.toLowerCase());
        featuresOfTypeList.addAll(xrefIds);
        if (dbType == UniprotKBFeatureDatabase.CHEBI) {
            String id = xrefId;

            if (id.startsWith(CHEBI2)) {
                id = id.substring(CHEBI2.length());
                ChebiEntry chebi = chebiRepo.getById(id);
                if (notNull(chebi)) {
                    addChebiSuggestions(SuggestDictionary.CHEBI, xrefId, chebi);
                }
                document.crossRefs.add(id);
            }
        }
    }

    private void addChebiSuggestions(SuggestDictionary dicType, String id, ChebiEntry chebi) {
        SuggestDocument.SuggestDocumentBuilder suggestionBuilder =
                SuggestDocument.builder().id(id).value(chebi.getName()).dictionary(dicType.name());
        if (!nullOrEmpty(chebi.getInchiKey())) {
            suggestionBuilder.altValue(chebi.getInchiKey());
        }
        suggestions.putIfAbsent(
                UniProtEntryConverterUtil.createSuggestionMapKey(dicType, id),
                suggestionBuilder.build());
    }

    private String getFeatureField(UniProtKBFeature feature, String type) {
        String field = type + feature.getType().name().toLowerCase();
        return field.replaceAll(" ", "_");
    }
}
