package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ChebiNodeEntryRelatedFieldsRowMapperTest {
    @Test
    void joinAndExtractLabelAndClassRelatedNodesFromNodeEntryRow() throws Exception {
        ChebiNodeEntryRelatedFieldsRowMapper mapper = new ChebiNodeEntryRelatedFieldsRowMapper();
        Row row = getRow();
        Iterator<Tuple2<String, Map<String, Seq<String>>>> result = mapper.call(row);
        Tuple2<String, Map<String, Seq<String>>> resultRow = result.next();
        assertNotNull(resultRow);
        assertEquals("http://purl.obolibrary.org/obo/CHEBI_74148", resultRow._1);
        assertTrue(String.valueOf(resultRow._2().get("name")).contains("2-hydroxybehenoyl-CoA"));
    }

    private Row getRow() {
        String aboutSubject = "http://purl.obolibrary.org/obo/CHEBI_74148";
        String subject = "name189730";
        Map<String, Object> object = new HashMap<>();
        object.put(
                "name",
                JavaConverters.asScalaBufferConverter(Arrays.asList("2-hydroxybehenoyl-CoA"))
                        .asScala()
                        .toList());

        Object[] rowValues = {aboutSubject, subject, JavaConverters.mapAsScalaMap(object)};
        return new GenericRowWithSchema(rowValues, getSchema());
    }

    public static StructType getSchema() {
        StructType schema =
                new StructType()
                        .add("about_subject", DataTypes.StringType)
                        .add("subject", DataTypes.StringType)
                        .add(
                                "object",
                                DataTypes.createMapType(
                                        DataTypes.StringType,
                                        DataTypes.createArrayType(DataTypes.StringType)));
        return schema;
    }
}
