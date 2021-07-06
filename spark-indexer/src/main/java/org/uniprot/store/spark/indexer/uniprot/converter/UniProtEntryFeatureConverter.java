package org.uniprot.store.spark.indexer.uniprot.converter;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.uniprot.core.uniprotkb.feature.UniProtKBFeature;
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

            if (feature.hasFeatureCrossReference()) {
                String xrefId = feature.getFeatureCrossReference().getId();
                String dbName = feature.getFeatureCrossReference().getDatabase().getName();
                featuresOfTypeList.addAll(UniProtEntryConverterUtil.getXrefId(xrefId, dbName));
                document.content.addAll(UniProtEntryConverterUtil.getXrefId(xrefId, dbName));
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
}
