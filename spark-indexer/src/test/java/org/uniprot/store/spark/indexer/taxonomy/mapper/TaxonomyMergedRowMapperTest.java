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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyUtils.getEntry;

class TaxonomyMergedRowMapperTest {

    @Test
    void mapMergedEntry() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // OLD_TAX_ID
        values.add(new BigDecimal(2000)); // NEW_TAX_ID
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyMergedSchema());


        TaxonomyMergedRowMapper mapper = new TaxonomyMergedRowMapper();
        TaxonomyDocument result = mapper.call(row);

        assertFalse(result.isActive());
        assertEquals("1000", result.getId());
        assertNotNull(result.getTaxonomyObj());
        TaxonomyEntry entry = getEntry(result.getTaxonomyObj());
        assertEquals(1000L, entry.getTaxonId());
        assertNotNull(entry.getInactiveReason());
        TaxonomyInactiveReason reason = entry.getInactiveReason();
        assertEquals(TaxonomyInactiveReasonType.MERGED, reason.getInactiveReasonType());
        assertEquals(2000L, reason.getMergedTo());
    }

    private StructType getTaxonomyMergedSchema() {
        StructType structType = new StructType();
        structType = structType.add("OLD_TAX_ID", DataTypes.LongType, true);
        structType = structType.add("NEW_TAX_ID", DataTypes.LongType, true);
        return structType;
    }

}