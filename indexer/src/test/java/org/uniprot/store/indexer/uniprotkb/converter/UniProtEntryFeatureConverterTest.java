package org.uniprot.store.indexer.uniprotkb.converter;

import org.junit.jupiter.api.Test;
import org.uniprot.core.DBCrossReference;
import org.uniprot.core.Range;
import org.uniprot.core.builder.DBCrossReferenceBuilder;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;
import org.uniprot.core.uniprot.evidence.builder.EvidenceBuilder;
import org.uniprot.core.uniprot.feature.AlternativeSequence;
import org.uniprot.core.uniprot.feature.Feature;
import org.uniprot.core.uniprot.feature.FeatureType;
import org.uniprot.core.uniprot.feature.FeatureXDbType;
import org.uniprot.core.uniprot.feature.builder.AlternativeSequenceBuilder;
import org.uniprot.core.uniprot.feature.builder.FeatureBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtEntryFeatureConverterTest {

    @Test
    void convertFeatureWithFilteredProteinsWith() {
        UniProtDocument document = new UniProtDocument();
        UniProtEntryFeatureConverter converter = new UniProtEntryFeatureConverter();

        Range location = Range.create(2, 8);
        Feature feature = new FeatureBuilder()
                .type(FeatureType.NON_TER)
                .location(location)
                .build();

        List<Feature> features = Collections.singletonList(feature);

        converter.convertFeature(features, document);

        assertEquals(Collections.emptySet(), document.proteinsWith);
    }


    @Test
    void convertFeature() {
        UniProtDocument document = new UniProtDocument();
        UniProtEntryFeatureConverter converter = new UniProtEntryFeatureConverter();

        List<Feature> features = Collections.singletonList(getFeature());

        converter.convertFeature(features, document);

        assertTrue(document.featuresMap.containsKey("ft_chain"));
        List<String> chainValue = Arrays.asList("CHAIN", "FT12345", "dbSNP-DBSNP-12345", "description value", "DBSNP-12345");
        assertEquals(new HashSet<>(chainValue), document.featuresMap.get("ft_chain"));

        assertTrue(document.featureEvidenceMap.containsKey("ftev_chain"));
        List<String> chainEvidenceValue = Arrays.asList("manual", "ECO_0000255");
        assertEquals(new HashSet<>(chainEvidenceValue), document.featureEvidenceMap.get("ftev_chain"));

        assertTrue(document.featureLengthMap.containsKey("ftlen_chain"));
        List<Integer> chainLengthValue = Collections.singletonList(7);
        assertEquals(new HashSet<>(chainLengthValue), document.featureLengthMap.get("ftlen_chain"));

        assertEquals(5, document.content.size());
        assertEquals(new HashSet<>(Arrays.asList("CHAIN", "FT12345", "dbSNP-DBSNP-12345",
                "description value", "DBSNP-12345")), document.content);

        assertEquals(Collections.singleton("chain"), document.proteinsWith);
    }

    private static Feature getFeature() {
        AlternativeSequence alternativeSequence = new AlternativeSequenceBuilder()
                .original("original value")
                .alternative("alternative value")
                .build();

        DBCrossReference<FeatureXDbType> xrefs = new DBCrossReferenceBuilder<FeatureXDbType>()
                .databaseType(FeatureXDbType.DBSNP)
                .id("DBSNP-12345")
                .build();

        Range location = Range.create(2, 8);
        List<Evidence> evidences = Collections.singletonList(createEvidence());
        return new FeatureBuilder()
                .type(FeatureType.CHAIN)
                .alternativeSequence(alternativeSequence)
                .dbXref(xrefs)
                .description("description value")
                .evidences(evidences)
                .featureId("FT12345")
                .location(location)
                .build();
    }

    private static Evidence createEvidence() {
        return new EvidenceBuilder()
                .evidenceCode(EvidenceCode.ECO_0000255)
                .databaseName("PROSITE-ProRule")
                .databaseId("PRU10020")
                .build();
    }
}