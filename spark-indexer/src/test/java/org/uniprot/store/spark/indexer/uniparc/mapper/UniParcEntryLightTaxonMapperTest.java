package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;
import org.uniprot.core.util.PairImpl;

import scala.Tuple2;

class UniParcEntryLightTaxonMapperTest {

    private UniParcEntryLightTaxonMapper mapper;

    @BeforeEach
    public void setUp() {
        mapper = new UniParcEntryLightTaxonMapper();
    }

    @Test
    void testCall() throws Exception {
        // Given
        UniParcEntryLight originalEntry =
                new UniParcEntryLightBuilder().uniParcId("UPI000000001").build();

        List<Tuple2<String, String>> commonTaxons =
                Arrays.asList(
                        new Tuple2<>("9606", "Homo sapiens"),
                        new Tuple2<>("10090", "Mus musculus"));

        Tuple2<UniParcEntryLight, List<Tuple2<String, String>>> input =
                new Tuple2<>(originalEntry, commonTaxons);

        // When
        UniParcEntryLight result = mapper.call(input);

        // Then
        assertEquals("UPI000000001", result.getUniParcId());
        assertTrue(result.getCommonTaxons().contains(new PairImpl<>("9606", "Homo sapiens")));
        assertTrue(result.getCommonTaxons().contains(new PairImpl<>("10090", "Mus musculus")));
        assertEquals(2, result.getCommonTaxons().size());
    }

    @Test
    void testCallWithEmptyTaxons() throws Exception {
        // Given
        UniParcEntryLight originalEntry =
                new UniParcEntryLightBuilder().uniParcId("UPI000000002").build();

        List<Tuple2<String, String>> commonTaxons = Arrays.asList();

        Tuple2<UniParcEntryLight, List<Tuple2<String, String>>> input =
                new Tuple2<>(originalEntry, commonTaxons);

        // When
        UniParcEntryLight result = mapper.call(input);

        // Then
        assertEquals("UPI000000002", result.getUniParcId());
        assertTrue(result.getCommonTaxons().isEmpty());
    }
}
