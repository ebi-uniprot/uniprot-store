package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

class ChebiPH7RelatedMapperTest {

    @Test
    void canMapChebiPH7RelatedIds() throws Exception {
        Row input = getRow();
        ChebiPH7RelatedMapper mapper = new ChebiPH7RelatedMapper();
        Tuple2<Long, Long> result = mapper.call(input);
        assertNotNull(result);
        assertEquals(22222L, result._1);
        assertEquals(11111L, result._2);
    }

    private Row getRow() {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("11111"); // CHEBI
        entryValues.add("22222"); // CHEBI_PH7_3

        return new GenericRowWithSchema(entryValues.toArray(), getRowSchema());
    }

    private StructType getRowSchema() {
        StructType structType = new StructType();
        structType = structType.add("CHEBI", DataTypes.StringType, true);
        structType = structType.add("CHEBI_PH7_3", DataTypes.StringType, true);
        return structType;
    }
}
