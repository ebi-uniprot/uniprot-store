package org.uniprot.store.spark.indexer.uniprot.converter;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.uniprot.core.CrossReference;
import org.uniprot.core.uniprotkb.feature.Ligand;
import org.uniprot.core.uniprotkb.feature.LigandPart;
import org.uniprot.core.uniprotkb.feature.UniProtKBFeature;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureDatabase;
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
            Collection<String> featuresOfTypeList =
                    document.featuresMap.computeIfAbsent(field, k -> new HashSet<>());

            featuresOfTypeList.add(feature.getType().getName());
            document.content.add(feature.getType().getName());

            if (feature.hasFeatureId()) {
                featuresOfTypeList.add(feature.getFeatureId().getValue());
                document.content.add(feature.getFeatureId().getValue());
            }
            if (feature.hasDescription()) {
                featuresOfTypeList.add(feature.getDescription().getValue());
                document.content.add(feature.getDescription().getValue());
            }

            ProteinsWith.from(feature.getType())
                    .map(ProteinsWith::getValue)
                    .filter(value -> !document.proteinsWith.contains(value)) // avoid duplicated
                    .ifPresent(value -> document.proteinsWith.add(value));

            addFeatureCrossReferences(feature, document, featuresOfTypeList);
            if (feature.hasLigand()) {
                Ligand ligand = feature.getLigand();
                featuresOfTypeList.add(ligand.getName());
                document.content.add(ligand.getName());
                //				if(Utils.notNullNotEmpty(ligand.getId())) {
                //					featuresOfTypeList.add(ligand.getId());
                //					document.content.add(ligand.getId());
                //				}
                if (Utils.notNullNotEmpty(ligand.getLabel())) {
                    featuresOfTypeList.add(ligand.getLabel());
                    document.content.add(ligand.getLabel());
                }
                if (Utils.notNullNotEmpty(ligand.getNote())) {
                    featuresOfTypeList.add(ligand.getNote());
                    document.content.add(ligand.getNote());
                }
            }
            if (feature.hasLigandPart()) {
                LigandPart ligandPart = feature.getLigandPart();
                featuresOfTypeList.add(ligandPart.getName());
                document.content.add(ligandPart.getName());
                //				if(Utils.notNullNotEmpty(ligandPart.getId())) {
                //					featuresOfTypeList.add(ligandPart.getId());
                //					document.content.add(ligandPart.getId());
                //				}
                if (Utils.notNullNotEmpty(ligandPart.getLabel())) {
                    featuresOfTypeList.add(ligandPart.getLabel());
                    document.content.add(ligandPart.getLabel());
                }
                if (Utils.notNullNotEmpty(ligandPart.getNote())) {
                    featuresOfTypeList.add(ligandPart.getNote());
                    document.content.add(ligandPart.getNote());
                }
            }

            // start and end of location
            int length =
                    feature.getLocation().getEnd().getValue()
                            - feature.getLocation().getStart().getValue()
                            + 1;
            Set<String> evidences =
                    UniProtEntryConverterUtil.extractEvidence(feature.getEvidences());
            Collection<Integer> lengthList =
                    document.featureLengthMap.computeIfAbsent(lengthField, k -> new HashSet<>());
            lengthList.add(length);

            Collection<String> evidenceList =
                    document.featureEvidenceMap.computeIfAbsent(evField, k -> new HashSet<>());
            evidenceList.addAll(evidences);
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

        List<String> xrefIds = UniProtEntryConverterUtil.getXrefId(xrefId, dbname);
        document.crossRefs.addAll(xrefIds);
        document.databases.add(dbname.toLowerCase());
        document.content.addAll(xrefIds);
        featuresOfTypeList.addAll(xrefIds);
    }
}
