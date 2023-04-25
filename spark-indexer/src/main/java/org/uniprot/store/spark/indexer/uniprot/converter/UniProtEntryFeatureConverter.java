package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.uniprot.core.CrossReference;
import org.uniprot.core.uniprotkb.feature.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.ProteinsWith;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
class UniProtEntryFeatureConverter {
    private static final String FEATURE = "ft_";
    private static final String FT_LENGTH = "ftlen_";
    private static final String FT_EV = "ftev_";

    void convertFeature(List<UniProtKBFeature> features, UniProtDocument document) {
        for (UniProtKBFeature feature : features) {
            String field = getFeatureField(feature, FEATURE);
            String lengthField = getFeatureField(feature, FT_LENGTH);
            String evField = getFeatureField(feature, FT_EV);
            Collection<String> featureValueList = new HashSet<>();

            featureValueList.add(feature.getType().getName());

            if (feature.hasFeatureId()) {
                featureValueList.add(feature.getFeatureId().getValue());
            }
            if (feature.hasDescription()) {
                featureValueList.add(feature.getDescription().getValue());
            }

            ProteinsWith.from(feature.getType())
                    .map(ProteinsWith::getValue)
                    .filter(value -> !document.proteinsWith.contains(value)) // avoid duplicated
                    .ifPresent(value -> document.proteinsWith.add(value));

            addFeatureCrossReferences(feature, document, featureValueList);
            if (feature.hasLigand()) {
                Ligand ligand = feature.getLigand();
                featureValueList.add(ligand.getName());
                if (Utils.notNullNotEmpty(ligand.getLabel())) {
                    featureValueList.add(ligand.getLabel());
                }
                if (Utils.notNullNotEmpty(ligand.getNote())) {
                    featureValueList.add(ligand.getNote());
                }
            }
            if (feature.hasLigandPart()) {
                LigandPart ligandPart = feature.getLigandPart();
                featureValueList.add(ligandPart.getName());
                if (Utils.notNullNotEmpty(ligandPart.getLabel())) {
                    featureValueList.add(ligandPart.getLabel());
                }
                if (Utils.notNullNotEmpty(ligandPart.getNote())) {
                    featureValueList.add(ligandPart.getNote());
                }
            }

            // start and end of location
            Set<String> evidences = extractEvidence(feature.getEvidences());

            document.featuresMap
                    .computeIfAbsent(field, k -> new HashSet<>())
                    .addAll(featureValueList);
            document.content.addAll(featureValueList);

            String featureValues = String.join(" ", featureValueList);
            if (canAddExperimental(
                    feature.getType().isAddExperimental(),
                    featureValues,
                    document.reviewed,
                    evidences)) {
                String experimentalField = field + "_exp";
                document.featuresMap
                        .computeIfAbsent(experimentalField, k -> new HashSet<>())
                        .addAll(featureValueList);
                document.evidenceExperimental = true;
            }
        }
    }

    private String getFeatureField(UniProtKBFeature feature, String type) {
        String field = type + feature.getType().name().toLowerCase();
        return field.replace(' ', '_');
    }

    private void addFeatureCrossReferences(
            UniProtKBFeature feature,
            UniProtDocument document,
            Collection<String> featuresOfTypeList) {
        if (Utils.notNullNotEmpty(feature.getFeatureCrossReferences())) {
            feature.getFeatureCrossReferences().stream()
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

        List<String> xrefIds = getXrefId(xrefId, dbname);
        if (!dbType.equals(UniprotKBFeatureDatabase.CHEBI)) {
            document.crossRefs.addAll(xrefIds);
            document.databases.add(dbname.toLowerCase());
        }
        document.content.addAll(xrefIds);
        featuresOfTypeList.addAll(xrefIds);
    }
}
