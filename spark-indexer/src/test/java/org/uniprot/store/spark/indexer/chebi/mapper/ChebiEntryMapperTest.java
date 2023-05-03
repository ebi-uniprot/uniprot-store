package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;

import scala.Tuple2;
import scala.collection.JavaConverters;

class ChebiEntryMapperTest {

    private ChebiEntryMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ChebiEntryMapper();
    }

    @Test
    void testNullMap() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_12345";
        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(null);
        Tuple2<Long, ChebiEntry> result = mapper.call(row);
        assertNull(result);
    }

    @Test
    void testProperlyFormattedRow() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_12345";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "chebiStructuredName",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1", "name2"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                "chebislash:inchikey",
                JavaConverters.asScalaBufferConverter(Arrays.asList("inchikey1"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                "rdfs:label",
                JavaConverters.asScalaBufferConverter(Arrays.asList("label1", "label2"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                "obo:IAO_0000115",
                JavaConverters.asScalaBufferConverter(Arrays.asList("is_a CHEBI:67890"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                "owl:onProperty",
                JavaConverters.asScalaBufferConverter(Arrays.asList("is_conjugate_acid_of"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                "owl:someValuesFrom",
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList("http://purl.obolibrary.org/obo/CHEBI_98765"))
                        .asScala()
                        .toList());
        scala.collection.Map<Object, Object> scalaMap =
                JavaConverters.mapAsScalaMapConverter(rawJavaMap).asScala();

        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(scalaMap);

        Tuple2<Long, ChebiEntry> result = mapper.call(row);
        assertEquals(12345L, result._1);
        ChebiEntry entry = result._2;
        assertEquals("label1", entry.getName());
        assertEquals("inchikey1", entry.getInchiKey());
        assertEquals(Arrays.asList("label2"), entry.getSynonyms());
        assertEquals(2, entry.getRelatedIds().size());
        assertEquals("67890", entry.getRelatedIds().get(0).getId());
        assertEquals("98765", entry.getRelatedIds().get(1).getId());
        assertEquals(0, entry.getMajorMicrospecies().size());
    }

    @Test
    void validateChebiWithMultiplesIsARelations() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_74245";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "obo:IAO_0000115",
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList("is_a CHEBI:65055 is_a CHEBI:74241"))
                        .asScala()
                        .toList());
        scala.collection.Map<Object, Object> scalaMap =
                JavaConverters.mapAsScalaMapConverter(rawJavaMap).asScala();

        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(scalaMap);
        Tuple2<Long, ChebiEntry> result = mapper.call(row);
        assertNotNull(result);
        ChebiEntry entry = result._2;
        assertEquals(2, entry.getRelatedIds().size());
        assertEquals("65055", entry.getRelatedIds().get(0).getId());
        assertEquals("74241", entry.getRelatedIds().get(1).getId());
    }

    @Test
    void validateChebiWithSynonyms() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_74245";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "rdfs:label",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1", "name2"))
                        .asScala()
                        .toList());
        scala.collection.Map<Object, Object> scalaMap =
                JavaConverters.mapAsScalaMapConverter(rawJavaMap).asScala();

        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(scalaMap);
        Tuple2<Long, ChebiEntry> result = mapper.call(row);
        assertNotNull(result);
        ChebiEntry entry = result._2;
        assertEquals("name1", entry.getName());
        assertEquals(Arrays.asList("name2"), entry.getSynonyms());
    }

    @Test
    void validateChebiWithMajorMicrospecies() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_77814";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "owl:onProperty",
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList("has_major_microspecies_at_pH_7_3"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                "owl:someValuesFrom",
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList("http://purl.obolibrary.org/obo/CHEBI_77793"))
                        .asScala()
                        .toList());
        scala.collection.Map<Object, Object> scalaMap =
                JavaConverters.mapAsScalaMapConverter(rawJavaMap).asScala();

        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(scalaMap);
        Tuple2<Long, ChebiEntry> result = mapper.call(row);
        assertNotNull(result);
        ChebiEntry entry = result._2;
        assertEquals(1, entry.getMajorMicrospecies().size());
        assertEquals("77793", entry.getMajorMicrospecies().get(0).getId());
    }
}
