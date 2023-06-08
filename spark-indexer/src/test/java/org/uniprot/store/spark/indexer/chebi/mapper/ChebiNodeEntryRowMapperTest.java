package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.indexer.common.utils.Constants.CHEBI_RDFS_LABEL_ATTRIBUTE;
import static org.uniprot.store.indexer.common.utils.Constants.CHEBI_RDF_RESOURCE_ATTRIBUTE;
import static org.uniprot.store.spark.indexer.chebi.ChebiOwlReader.getSchema;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryRowMapperTest.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import scala.collection.JavaConverters;

public class ChebiNodeEntryRowMapperTest {

    @Test
    void canMapChebiNodeEntryRowForName() throws Exception {
        ChebiNodeEntryRowMapper mapper = new ChebiNodeEntryRowMapper();
        Row row = getRowArrayListForNames();
        Row result = mapper.call(row);
        assertNotNull(result);
        assertEquals("name189730", result.get(0));
        assertTrue(result.get(1).toString().contains("2-hydroxybehenoyl-CoA"));
    }

    @Test
    void canMapChebiNodeEntryRowForSynonym() throws Exception {
        ChebiNodeEntryRowMapper mapper = new ChebiNodeEntryRowMapper();
        Row row = getRowArrayListForSynonyms();
        Row result = mapper.call(row);
        assertNotNull(result);
        Map<String, String> map = getRowMap(result);
        assertEquals("name189731", result.get(0));
        assertEquals("2-hydroxybehenoyl-coenzyme A", map.get("rdfs:label"));
    }

    @Test
    void canMapChebiNodeEntryRowForRdfsSubclass() throws Exception {
        ChebiNodeEntryRowMapper mapper = new ChebiNodeEntryRowMapper();
        Row row = getRowArrayListForSubClassRelatedId();
        Row result = mapper.call(row);
        assertNotNull(result);
        Map<String, String> map = getRowMap(result);
        assertEquals("bn74148tmms73f74117", result.get(0));
        assertEquals("http://purl.obolibrary.org/obo/CHEBI_74117", map.get("owl:someValuesFrom"));
        assertEquals(
                "http://purl.obolibrary.org/obo/chebi#has_major_microspecies_at_pH_7_3",
                map.get("owl:onProperty"));
    }

    public static Row getRowArrayListForNames() {
        List<Object> values = new ArrayList<>();
        values.add(null); // _rdf:about
        values.add("name189730"); // _rdf:nodeID
        values.add(null); // name
        List<Row> rdfType =
                Arrays.asList(
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                "http://purl.uniprot.org/core/ChEBI_Common_Name")); // rdf:type
        values.add(JavaConverters.asScalaIteratorConverter(rdfType.iterator()).asScala().toSeq());
        values.add(null); // chebiStructuredName
        values.add(null); // chebislash:inchikey
        values.add(null); // obo:IAO_0000115
        values.add(null); // oboInOwl:hasId
        values.add(null); // rdfs:subClassOf
        List<Row> rdfsLabel =
                Arrays.asList(
                        RowFactory.create(CHEBI_RDFS_LABEL_ATTRIBUTE, "2-hydroxybehenoyl-CoA"));
        values.add(
                JavaConverters.asScalaIteratorConverter(rdfsLabel.iterator())
                        .asScala()
                        .toSeq()); // rdfs:label
        values.add(null); // owl:onProperty
        values.add(null); // owl:someValuesFrom

        Object[] rowValues = values.toArray();
        StructType schema = getSchema();
        return new GenericRowWithSchema(rowValues, schema);
    }

    public static Row getRowArrayListForSynonyms() {
        List<Object> values = new ArrayList<>();
        values.add(null); // _rdf:about
        values.add("name189731"); // _rdf:nodeID
        values.add(null); // name
        values.add(null); // rdf:type
        values.add(null); // chebiStructuredName
        values.add(null); // chebislash:inchikey
        values.add(null); // obo:IAO_0000115
        values.add(null); // oboInOwl:hasId
        values.add(null); // rdfs:subClassOf
        values.add(
                JavaConverters.collectionAsScalaIterableConverter(
                                Arrays.asList(
                                        new Object[] { // rdfs:label
                                            "2-hydroxybehenoyl-coenzyme A"
                                        }))
                        .asScala()
                        .toSeq());
        values.add(null); // owl:onProperty
        values.add(null); // owl:someValuesFrom

        Object[] rowValues = values.toArray();
        StructType schema = getSchema();
        return new GenericRowWithSchema(rowValues, schema);
    }

    public static Row getRowArrayListForSubClassRelatedId() {
        List<Object> values = new ArrayList<>();
        values.add(null); // _rdf:about
        values.add("bn74148tmms73f74117"); // _rdf:nodeID
        values.add(null); // name
        List<Row> rdfType =
                Arrays.asList(
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                "http://www.w3.org/2002/07/owl#Restriction")); // rdf:type
        values.add(JavaConverters.asScalaIteratorConverter(rdfType.iterator()).asScala().toSeq());
        values.add(null); // chebiStructuredName
        values.add(null); // chebislash:inchikey
        values.add(null); // obo:IAO_0000115
        values.add(null); // oboInOwl:hasId
        values.add(null); // rdfs:subClassOf
        values.add(null); // rdfs:label
        List<Row> someProperty =
                Arrays.asList(
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                "http://purl.obolibrary.org/obo/chebi#has_major_microspecies_at_pH_7_3"));
        values.add(
                JavaConverters.asScalaIteratorConverter(someProperty.iterator())
                        .asScala()
                        .toSeq()); // owl:someProperty
        List<Row> someValuesFrom =
                Arrays.asList(
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                "http://purl.obolibrary.org/obo/CHEBI_74117"));
        values.add(
                JavaConverters.asScalaIteratorConverter(someValuesFrom.iterator())
                        .asScala()
                        .toSeq()); // owl:someValuesFrom

        Object[] rowValues = values.toArray();
        StructType schema = getSchema();
        return new GenericRowWithSchema(rowValues, schema);
    }
}
