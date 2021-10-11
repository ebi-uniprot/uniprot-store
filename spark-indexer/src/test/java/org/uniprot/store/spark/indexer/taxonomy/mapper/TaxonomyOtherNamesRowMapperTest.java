package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyInactiveReason;
import org.uniprot.core.taxonomy.TaxonomyInactiveReasonType;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyUtils.getEntry;

class TaxonomyOtherNamesRowMapperTest {


    @Test
    void mapOtherName() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // TAX_ID
        values.add(new BigDecimal(1)); // PRIORITY
        values.add("nameValue"); // NAME
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyOtherNamesSchema());

        TaxonomyOtherNamesRowMapper mapper = new TaxonomyOtherNamesRowMapper();
        Tuple2<String, TaxonomyEntry> result = mapper.call(row);
        assertNotNull(result);
        assertEquals("1000", result._1);
        assertNotNull(result._2);
        assertNotNull(result._2.getOtherNames());
        assertEquals(1, result._2.getOtherNames().size());
        assertTrue(result._2.getOtherNames().contains("nameValue"));
    }


    @Test
    void doNotMapNegativePriority() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // TAX_ID
        values.add(new BigDecimal(-1)); // PRIORITY
        values.add("nameValue"); // NAME
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyOtherNamesSchema());

        TaxonomyOtherNamesRowMapper mapper = new TaxonomyOtherNamesRowMapper();
        Tuple2<String, TaxonomyEntry> result = mapper.call(row);
        assertNotNull(result);
        assertEquals("1000", result._1);
        assertNotNull(result._2);
        assertNotNull(result._2.getOtherNames());
        assertEquals(1, result._2.getOtherNames().size());
        assertTrue(result._2.getOtherNames().get(0).isEmpty());
    }

    private StructType getTaxonomyOtherNamesSchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.LongType, true);
        structType = structType.add("PRIORITY", DataTypes.LongType, true);
        structType = structType.add("NAME", DataTypes.StringType, true);
        return structType;
    }
}