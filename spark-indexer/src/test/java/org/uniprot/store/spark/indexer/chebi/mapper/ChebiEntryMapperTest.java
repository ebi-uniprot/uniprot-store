package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.uniprot.store.indexer.common.utils.Constants.*;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryMapper.RELATED_MICROSPECIES_PREFIX;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryMapper.RELATED_MICROSPECIES_URL;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryMapper.RO_0018033;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryMapper.RO_0018034;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("label1")).asScala().toList());
        rawJavaMap.put(
                CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1", "name2"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                "chemrof:inchi_key_string",
                JavaConverters.asScalaBufferConverter(Arrays.asList("inchikey1"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                CHEBI_RDFS_LABEL_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(Arrays.asList("label2")).asScala().toList());
        rawJavaMap.put(
                "rdfs:subClassOf",
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList(
                                        "http://purl.obolibrary.org/obo/CHEBI_3300",
                                        "http://purl.obolibrary.org/obo/CHEBI_3400"))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(Arrays.asList(RO_0018034))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE,
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
        assertEquals(3, entry.getRelatedIds().size());
        assertEquals("3300", entry.getRelatedIds().get(0).getId());
        assertEquals("3400", entry.getRelatedIds().get(1).getId());
        assertEquals("98765", entry.getRelatedIds().get(2).getId());
        assertEquals(0, entry.getMajorMicrospecies().size());
    }

    @Test
    void validateChebiWithMultiplesIsARelations() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_74245";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1")).asScala().toList());
        rawJavaMap.put(
                "rdfs:subClassOf",
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList(
                                        "http://purl.obolibrary.org/obo/CHEBI_3300",
                                        "http://purl.obolibrary.org/obo/CHEBI_3400"))
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
        assertEquals("3300", entry.getRelatedIds().get(0).getId());
        assertEquals("3400", entry.getRelatedIds().get(1).getId());
    }

    @Test
    void validateChebiWithSynonyms() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_74245";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1")).asScala().toList());
        rawJavaMap.put(
                CHEBI_RDFS_LABEL_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(Arrays.asList("name2", "name3"))
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
        assertEquals(Arrays.asList("name2", "name3"), entry.getSynonyms());
    }

    @Test
    void validateChebiWithMajorMicrospecies() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_77814";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1")).asScala().toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(Arrays.asList(RELATED_MICROSPECIES_PREFIX))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE,
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

    @ParameterizedTest
    @ValueSource(strings = {RO_0018033, RO_0018034})
    void validateChebiWithChebi2ConjugateRelation(String relation) throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_77814";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1")).asScala().toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList("http://purl.obolibrary.org/obo/RO_" + relation))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList("http://purl.obolibrary.org/obo/CHEBI_77793"))
                        .asScala()
                        .toList());
        scala.collection.Map<Object, Object> scalaMap =
                JavaConverters.mapAsScalaMapConverter(rawJavaMap).asScala();

        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(scalaMap);

        ChebiEntry entry = mapper.call(row)._2;
        assertEquals(1, entry.getRelatedIds().size());
        assertEquals("77793", entry.getRelatedIds().get(0).getId());
    }

    @Test
    void validateChebiWithFullMajorMicrospeciesPropertyUrl() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_77814";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1")).asScala().toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(Arrays.asList(RELATED_MICROSPECIES_URL))
                        .asScala()
                        .toList());
        rawJavaMap.put(
                CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE,
                JavaConverters.asScalaBufferConverter(
                                Arrays.asList("http://purl.obolibrary.org/obo/CHEBI_77793"))
                        .asScala()
                        .toList());
        scala.collection.Map<Object, Object> scalaMap =
                JavaConverters.mapAsScalaMapConverter(rawJavaMap).asScala();

        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(scalaMap);

        ChebiEntry entry = mapper.call(row)._2;
        assertEquals(1, entry.getMajorMicrospecies().size());
        assertEquals("77793", entry.getMajorMicrospecies().get(0).getId());
    }

    @Test
    void validateChebiWithoutChemrofInchiKey() throws Exception {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_77814";
        java.util.Map<Object, Object> rawJavaMap = new HashMap<>();
        rawJavaMap.put(
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("name1")).asScala().toList());
        scala.collection.Map<Object, Object> scalaMap =
                JavaConverters.mapAsScalaMapConverter(rawJavaMap).asScala();

        Row row = mock(Row.class);
        when(row.getAs("subject")).thenReturn(subject);
        when(row.getMap(1)).thenReturn(scalaMap);

        ChebiEntry entry = mapper.call(row)._2;
        assertEquals("", entry.getInchiKey());
    }
}
