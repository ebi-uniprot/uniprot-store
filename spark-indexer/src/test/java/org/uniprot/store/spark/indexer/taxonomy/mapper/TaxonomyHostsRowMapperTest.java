package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

class TaxonomyHostsRowMapperTest {

    @Test
    void canMapTaxonomyHost() throws Exception {
        TaxonomyHostsRowMapper mapper = new TaxonomyHostsRowMapper();
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // TAX_ID
        values.add(new BigDecimal(2000)); // HOST_ID
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyHostSchema());

        Tuple2<String, String> result = mapper.call(row);
        assertNotNull(result);
        assertEquals("2000", result._1);
        assertEquals("1000", result._2);
    }

    private StructType getTaxonomyHostSchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.LongType, true);
        structType = structType.add("HOST_ID", DataTypes.LongType, true);
        return structType;
    }
}
