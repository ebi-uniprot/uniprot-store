package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import scala.collection.JavaConverters;

public class ChebiEntryRelatedFieldsRowMapperTest {

    @Test
    void verifyRelatedFieldsEntriesInARow() throws Exception {
        ChebiEntryRelatedFieldsRowMapper mapper = new ChebiEntryRelatedFieldsRowMapper();
        Row row = getRowWithSchema();
        Iterator<Row> result = mapper.call(row);
        Row resultRow1 = result.next();
        assertNotNull(resultRow1);
        assertEquals("http://purl.obolibrary.org/obo/CHEBI_74148", resultRow1.get(0));
        assertEquals("name189730", resultRow1.get(1));

        Row resultRow2 = result.next();
        assertNotNull(resultRow2);
        assertEquals("name189731", resultRow2.get(1));

        Row resultRow3 = result.next();
        assertNotNull(resultRow3);
        assertEquals("bn74148tmms73f74117", resultRow3.get(2));

        Row resultRow4 = result.next();
        assertNotNull(resultRow4);
        assertEquals("http://purl.obolibrary.org/obo/CHEBI_33184", resultRow4.get(2));
    }

    private Row getRowWithSchema() {
        String subject = "http://purl.obolibrary.org/obo/CHEBI_74148";
        java.util.Map<String, Object> objectMap = new java.util.HashMap<>();
        objectMap.put(
                "chebiStructuredName",
                JavaConverters.collectionAsScalaIterableConverter(
                                Arrays.asList("name189730", "name189731"))
                        .asScala()
                        .toSeq());
        objectMap.put("chebislash:inchikey", Arrays.asList("TXHBQUJRFDOFJT-FBLBILBLSA-N"));
        objectMap.put(
                "obo:IAO_0000115",
                Arrays.asList(
                        "A hydroxy fatty-acyl-CoA that results from the formal condensation of the thiol group of coenzyme A with the carboxy group of 2-hydroxybehenic acid."));
        objectMap.put("oboInOwl:hasId", Arrays.asList("74148"));
        objectMap.put(
                "rdfs:subClassOf",
                JavaConverters.collectionAsScalaIterableConverter(
                                Arrays.asList(
                                        "bn74148tmms73f74117",
                                        "http://purl.obolibrary.org/obo/CHEBI_33184"))
                        .asScala()
                        .toSeq());
        // Convert the objectMap to a Scala Map
        scala.collection.immutable.Map<String, Object> objectScalaMap =
                JavaConverters.mapAsScalaMapConverter(objectMap)
                        .asScala()
                        .toMap(scala.Predef.conforms());
        // Create values list for the Row
        List<Object> values = new ArrayList<>();
        values.add(subject);
        values.add(objectScalaMap);
        // Create the Row
        Object[] rowValues = values.toArray();
        return new GenericRowWithSchema(rowValues, getProcessedSchema());
    }

    private StructType getProcessedSchema() {
        StructType processedSchema =
                new StructType()
                        .add("about_subject", DataTypes.StringType)
                        .add(
                                "object",
                                DataTypes.createMapType(
                                        DataTypes.StringType,
                                        DataTypes.createArrayType(DataTypes.StringType)));
        return processedSchema;
    }
}
