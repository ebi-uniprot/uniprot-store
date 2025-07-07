package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.CommonOrganismBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;

import scala.Tuple2;
import scala.Tuple3;

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

        List<Tuple3<String, Long, String>> commonTaxons =
                Arrays.asList(
                        new Tuple3<>("9606", 9607L, "Homo sapiens"),
                        new Tuple3<>("10090", 10091L, "Mus musculus"));

        Tuple2<UniParcEntryLight, Optional<List<Tuple3<String, Long, String>>>> input =
                new Tuple2<>(originalEntry, Optional.of(commonTaxons));

        // When
        UniParcEntryLight result = mapper.call(input);

        // Then
        assertEquals("UPI000000001", result.getUniParcId());
        assertTrue(
                result.getCommonTaxons()
                        .contains(
                                new CommonOrganismBuilder()
                                        .topLevel("9606")
                                        .commonTaxon("Homo sapiens")
                                        .commonTaxonId(9607L)
                                        .build()));
        assertTrue(
                result.getCommonTaxons()
                        .contains(
                                new CommonOrganismBuilder()
                                        .topLevel("10090")
                                        .commonTaxonId(10091L)
                                        .commonTaxon("Mus musculus")
                                        .build()));
        assertEquals(2, result.getCommonTaxons().size());
    }

    @Test
    void testCallWithEmptyTaxons() throws Exception {
        // Given
        UniParcEntryLight originalEntry =
                new UniParcEntryLightBuilder().uniParcId("UPI000000002").build();

        List<Tuple3<String, Long, String>> commonTaxons = Arrays.asList();

        Tuple2<UniParcEntryLight, Optional<List<Tuple3<String, Long, String>>>> input =
                new Tuple2<>(originalEntry, Optional.of(commonTaxons));

        // When
        UniParcEntryLight result = mapper.call(input);

        // Then
        assertEquals("UPI000000002", result.getUniParcId());
        assertTrue(result.getCommonTaxons().isEmpty());
    }

    @Test
    void testCallWithEmptyList() throws Exception {
        // Given
        UniParcEntryLight originalEntry =
                new UniParcEntryLightBuilder().uniParcId("UPI000000002").build();

        Tuple2<UniParcEntryLight, Optional<List<Tuple3<String, Long, String>>>> input =
                new Tuple2<>(originalEntry, Optional.empty());

        // When
        UniParcEntryLight result = mapper.call(input);

        // Then
        assertEquals("UPI000000002", result.getUniParcId());
        assertTrue(result.getCommonTaxons().isEmpty());
    }
}
