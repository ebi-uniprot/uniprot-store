package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.CrossReference;
import org.uniprot.core.feature.AlternativeSequence;
import org.uniprot.core.feature.FeatureLocation;
import org.uniprot.core.feature.impl.AlternativeSequenceBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.feature.*;
import org.uniprot.core.uniprotkb.feature.impl.UniProtKBFeatureBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtKBEntryFeatureConverterTest {

    @Test
    void convertFeatureWithFilteredProteinsWith() {
        UniProtDocument document = new UniProtDocument();
        UniProtEntryFeatureConverter converter = new UniProtEntryFeatureConverter();

        FeatureLocation location = new FeatureLocation(2, 8);
        UniProtKBFeature feature =
                new UniProtKBFeatureBuilder()
                        .type(UniprotKBFeatureType.NON_TER)
                        .location(location)
                        .build();

        List<UniProtKBFeature> features = Collections.singletonList(feature);

        converter.convertFeature(features, document);

        assertEquals(Collections.emptySet(), document.proteinsWith);
    }

    @Test
    void convertFeature() {
        UniProtDocument document = new UniProtDocument();
        UniProtEntryFeatureConverter converter = new UniProtEntryFeatureConverter();

        List<UniProtKBFeature> features = Collections.singletonList(getFeature());

        converter.convertFeature(features, document);

        assertTrue(document.featuresMap.containsKey("ft_chain"));
        List<String> chainValue =
                Arrays.asList(
                        "CHAIN",
                        "FT12345",
                        "dbSNP-DBSNP-12345",
                        "description value",
                        "DBSNP-12345");
        assertEquals(new HashSet<>(chainValue), document.featuresMap.get("ft_chain"));

        assertTrue(document.featureEvidenceMap.containsKey("ftev_chain"));
        List<String> chainEvidenceValue = Arrays.asList("manual", "ECO_0000255");
        assertEquals(
                new HashSet<>(chainEvidenceValue), document.featureEvidenceMap.get("ftev_chain"));

        assertTrue(document.featureLengthMap.containsKey("ftlen_chain"));
        List<Integer> chainLengthValue = Collections.singletonList(7);
        assertEquals(new HashSet<>(chainLengthValue), document.featureLengthMap.get("ftlen_chain"));

        assertEquals(5, document.content.size());
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "CHAIN",
                                "FT12345",
                                "dbSNP-DBSNP-12345",
                                "description value",
                                "DBSNP-12345")),
                document.content);

        assertEquals(Collections.singleton("chain"), document.proteinsWith);
    }

    private static UniProtKBFeature getFeature() {
        AlternativeSequence alternativeSequence =
                new AlternativeSequenceBuilder()
                        .original("original value")
                        .alternativeSequencesAdd("alternative value")
                        .build();

        CrossReference<UniprotKBFeatureDatabase> xrefs =
                new CrossReferenceBuilder<UniprotKBFeatureDatabase>()
                        .database(UniprotKBFeatureDatabase.DBSNP)
                        .id("DBSNP-12345")
                        .build();

        FeatureLocation location = new FeatureLocation(2, 8);
        List<Evidence> evidences = Collections.singletonList(createEvidence());
        return new UniProtKBFeatureBuilder()
                .type(UniprotKBFeatureType.CHAIN)
                .alternativeSequence(alternativeSequence)
                .featureCrossReference(xrefs)
                .description("description value")
                .evidencesSet(evidences)
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
