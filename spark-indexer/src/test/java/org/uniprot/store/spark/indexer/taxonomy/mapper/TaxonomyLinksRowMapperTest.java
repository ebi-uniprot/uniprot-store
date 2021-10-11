package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TaxonomyLinksRowMapperTest {

    @Test
    void canMapLink() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // TAX_ID
        values.add("uriValue"); // URI
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyLinksSchema());

        TaxonomyLinksRowMapper mapper = new TaxonomyLinksRowMapper();
        Tuple2<String, TaxonomyEntry> result = mapper.call(row);
        assertNotNull(result);
        assertEquals("1000", result._1);
        assertNotNull(result._2.getLinks());
        assertEquals(1, result._2.getLinks().size());
        assertTrue(result._2.getLinks().contains("uriValue"));
    }

    private StructType getTaxonomyLinksSchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.LongType, true);
        structType = structType.add("URI", DataTypes.StringType, true);
        return structType;
    }
}