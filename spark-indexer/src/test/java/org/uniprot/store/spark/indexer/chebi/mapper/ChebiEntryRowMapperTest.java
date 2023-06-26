package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.indexer.common.utils.Constants.CHEBI_RDF_RESOURCE_ATTRIBUTE;
import static org.uniprot.store.spark.indexer.chebi.ChebiOwlReader.getSchema;

import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.convert.Wrappers;

public class ChebiEntryRowMapperTest {
    @Test
    void testGetChebiKeyValuesForRelatedAbstractSeq() throws Exception {
        ChebiEntryRowMapper mapper = new ChebiEntryRowMapper();
        Row row = getChebiKeyValuesForRelatedAbstractSeq();
        Row result = mapper.call(row);
        assertNotNull(result);

        Map<String, String> map = getRowMap(result);
        assertEquals("http://purl.obolibrary.org/obo/CHEBI_74148", result.get(0));
        assertEquals("TXHBQUJRFDOFJT-FBLBILBLSA-N", map.get("chebislash:inchikey"));
        assertEquals("74148", map.get("oboInOwl:hasId"));
    }

    @Test
    void testGetChebiKeyValuesFromStringResourceObj() throws Exception {
        ChebiEntryRowMapper mapper = new ChebiEntryRowMapper();
        Row row = getChebiKeyValuesFromStringResourceObj();
        Row result = mapper.call(row);
        assertNotNull(result);

        Map<String, String> map = getRowMap(result);
        assertEquals("http://purl.obolibrary.org/obo/CHEBI_74148", result.get(0));
        assertEquals("TXHBQUJRFDOFJT-FBLBILBLSA-N", map.get("chebislash:inchikey"));
        assertEquals("74148", map.get("oboInOwl:hasId"));
    }

    public static Row getChebiKeyValuesForRelatedAbstractSeq() {
        List<Object> values = new ArrayList<>();
        values.add("http://purl.obolibrary.org/obo/CHEBI_74148"); // _rdf:about
        values.add(null); // _rdf:nodeID
        values.add(null); // name
        values.add(
                Arrays.asList(
                        CHEBI_RDF_RESOURCE_ATTRIBUTE,
                        "http://www.w3.org/2002/07/owl#Class")); // rdf:type
        List<Row> structuredNames =
                Arrays.asList(
                        RowFactory.create("_rdf:nodeID", "name189730"),
                        RowFactory.create("_rdf:nodeID", "name189731"));
        values.add(
                JavaConverters.asScalaIteratorConverter(structuredNames.iterator())
                        .asScala()
                        .toSeq()); // chebiStructuredName
        values.add("TXHBQUJRFDOFJT-FBLBILBLSA-N"); // chebislash:inchikey
        values.add(
                "A hydroxy fatty-acyl-CoA that results from the formal condensation of the thiol group of coenzyme A with the carboxy group of 2-hydroxybehenic acid."); // obo:IAO_0000115
        values.add("74148"); // oboInOwl:hasId
        List<Row> subClassOf =
                Arrays.asList(
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                null,
                                "_rdf:nodeID",
                                "bn74148tmms73f74117"),
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                "http://purl.obolibrary.org/obo/CHEBI_33184",
                                "_rdf:nodeID",
                                null));
        values.add(subClassOf); // rdfs:subClassOf
        values.add(null); // rdfs:label
        values.add(null); // owl:onProperty
        values.add(null); // owl:someValuesFrom

        Object[] rowValues = values.toArray();
        StructType schema = getSchema();
        return new GenericRowWithSchema(rowValues, schema);
    }

    public static Row getChebiKeyValuesFromStringResourceObj() {
        List<Object> values = new ArrayList<>();
        values.add("http://purl.obolibrary.org/obo/CHEBI_74148"); // _rdf:about
        values.add(null); // _rdf:nodeID
        values.add(null); // name
        values.add(
                Arrays.asList(
                        CHEBI_RDF_RESOURCE_ATTRIBUTE,
                        "http://www.w3.org/2002/07/owl#Class")); // rdf:type
        List<Row> structuredNames =
                Arrays.asList(
                        RowFactory.create("_rdf:nodeID", "name189730"),
                        RowFactory.create("_rdf:nodeID", "name189731"));
        values.add(structuredNames.toString()); // chebiStructuredName
        values.add("TXHBQUJRFDOFJT-FBLBILBLSA-N"); // chebislash:inchikey
        values.add(
                "A hydroxy fatty-acyl-CoA that results from the formal condensation of the thiol group of coenzyme A with the carboxy group of 2-hydroxybehenic acid."); // obo:IAO_0000115
        values.add("74148"); // oboInOwl:hasId
        List<Row> subClassOf =
                Arrays.asList(
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                null,
                                "_rdf:nodeID",
                                "bn74148tmms73f74117"),
                        RowFactory.create(
                                CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                "http://purl.obolibrary.org/obo/CHEBI_33184",
                                "_rdf:nodeID",
                                null));
        values.add(subClassOf); // rdfs:subClassOf
        values.add(null); // rdfs:label
        values.add(null); // owl:onProperty
        values.add(null); // owl:someValuesFrom

        Object[] rowValues = values.toArray();
        StructType schema = getSchema();
        return new GenericRowWithSchema(rowValues, schema);
    }

    @NotNull
    public static Map<String, String> getRowMap(Row result) {
        Wrappers.JMapWrapper mapWrapper = (Wrappers.JMapWrapper) result.get(1);
        scala.collection.Iterator<Tuple2<String, Object>> iterator = mapWrapper.iterator();
        Map<String, String> map = new HashMap<>();
        while (iterator.hasNext()) {
            Tuple2<String, Object> entry = iterator.next();
            String key = entry._1();
            Object value = entry._2();
            if (value instanceof Wrappers.JListWrapper) {
                Wrappers.JListWrapper<String> listWrapper = (Wrappers.JListWrapper<String>) value;
                List<String> javaList = JavaConverters.seqAsJavaListConverter(listWrapper).asJava();
                map.put(key, javaList.get(0));
            } else {
                map.put(key, value.toString());
            }
        }
        return map;
    }
}
