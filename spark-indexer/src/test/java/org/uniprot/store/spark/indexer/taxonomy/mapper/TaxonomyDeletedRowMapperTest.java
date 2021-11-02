package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyUtils.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyInactiveReason;
import org.uniprot.core.taxonomy.TaxonomyInactiveReasonType;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

class TaxonomyDeletedRowMapperTest {

    @Test
    void mapDeletedEntry() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // TAX_ID
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyDeletedSchema());

        TaxonomyDeletedRowMapper mapper = new TaxonomyDeletedRowMapper();
        TaxonomyDocument result = mapper.call(row);

        assertNotNull(result.getTaxonomyObj());
        assertEquals("1000", result.getId());
        assertFalse(result.isActive());
        TaxonomyEntry entry = getEntry(result.getTaxonomyObj());
        assertEquals(1000L, entry.getTaxonId());
        assertNotNull(entry.getInactiveReason());
        TaxonomyInactiveReason reason = entry.getInactiveReason();
        assertEquals(TaxonomyInactiveReasonType.DELETED, reason.getInactiveReasonType());
        assertEquals(0L, reason.getMergedTo());
    }

    private StructType getTaxonomyDeletedSchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.LongType, true);
        return structType;
    }
}
