package indexer.taxonomy;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyRank;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyRowMapperTest {

    @Test
    void testTaxonomyRowMapperRequiredOnly() throws Exception{
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); //TAX_ID
        values.add(null); //SPTR_COMMON
        values.add(null); //NCBI_COMMON
        values.add(null); //SPTR_SCIENTIFIC
        values.add(null); //NCBI_SCIENTIFIC
        values.add(null); //TAX_CODE
        values.add(null); //PARENT_ID
        values.add(null); //RANK
        values.add(null); //SPTR_SYNONYM
        values.add(null); //HIDDEN

        Row row = new GenericRowWithSchema(values.toArray(),getTaxonomySchema() );

        TaxonomyRowMapper taxonomyRowMapper = new TaxonomyRowMapper();
        Tuple2<String, TaxonomyEntry> result = taxonomyRowMapper.call(row);

        assertNotNull(result);

        assertEquals("1000", result._1);
        TaxonomyEntry mappedEntry = result._2;
        assertNotNull(mappedEntry);

        assertEquals(1000L, mappedEntry.getTaxonId());
        assertEquals("", mappedEntry.getScientificName());
        assertEquals("", mappedEntry.getCommonName());
        assertNull(mappedEntry.getMnemonic());
        assertNull(mappedEntry.getParentId());
        assertEquals(TaxonomyRank.NO_RANK, mappedEntry.getRank());
        assertTrue(mappedEntry.getSynonyms().isEmpty());
        assertFalse(mappedEntry.isHidden());
    }

    @Test
    void testTaxonomyRowMapperWithSPRT() throws Exception{
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); //TAX_ID
        values.add("SPTR_COMMON value"); //SPTR_COMMON
        values.add("NCBI_COMMON value"); //NCBI_COMMON
        values.add("SPTR_SCIENTIFIC value"); //SPTR_SCIENTIFIC
        values.add("NCBI_SCIENTIFIC value"); //NCBI_SCIENTIFIC
        values.add("TAX_CODE value"); //TAX_CODE
        values.add(new BigDecimal(1001)); //PARENT_ID
        values.add("family"); //RANK
        values.add("SPTR_SYNONYM value"); //SPTR_SYNONYM
        values.add(new BigDecimal(1)); //HIDDEN

        Row row = new GenericRowWithSchema(values.toArray(),getTaxonomySchema() );

        TaxonomyRowMapper taxonomyRowMapper = new TaxonomyRowMapper();
        Tuple2<String, TaxonomyEntry> result = taxonomyRowMapper.call(row);

        assertNotNull(result);

        assertEquals("1000", result._1);
        TaxonomyEntry mappedEntry = result._2;
        assertNotNull(mappedEntry);

        assertEquals(1000L, mappedEntry.getTaxonId());
        assertEquals("SPTR_SCIENTIFIC value", mappedEntry.getScientificName());
        assertEquals("SPTR_COMMON value", mappedEntry.getCommonName());
        assertEquals("TAX_CODE value", mappedEntry.getMnemonic());
        assertEquals(1001L, mappedEntry.getParentId());
        assertEquals(TaxonomyRank.FAMILY, mappedEntry.getRank());
        assertTrue(mappedEntry.getSynonyms().contains("SPTR_SYNONYM value"));
        assertTrue(mappedEntry.isHidden());
    }

    @Test
    void testTaxonomyRowMapperWithNCBI() throws Exception{
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); //TAX_ID
        values.add(null); //SPTR_COMMON
        values.add("NCBI_COMMON value"); //NCBI_COMMON
        values.add(null); //SPTR_SCIENTIFIC
        values.add("NCBI_SCIENTIFIC value"); //NCBI_SCIENTIFIC
        values.add("TAX_CODE value"); //TAX_CODE
        values.add(new BigDecimal(1001)); //PARENT_ID
        values.add("family"); //RANK
        values.add("SPTR_SYNONYM value"); //SPTR_SYNONYM
        values.add(new BigDecimal(1)); //HIDDEN

        Row row = new GenericRowWithSchema(values.toArray(),getTaxonomySchema() );

        TaxonomyRowMapper taxonomyRowMapper = new TaxonomyRowMapper();
        Tuple2<String, TaxonomyEntry> result = taxonomyRowMapper.call(row);

        assertNotNull(result);

        assertEquals("1000", result._1);
        TaxonomyEntry mappedEntry = result._2;
        assertNotNull(mappedEntry);

        assertEquals(1000L, mappedEntry.getTaxonId());
        assertEquals("NCBI_SCIENTIFIC value", mappedEntry.getScientificName());
        assertEquals("NCBI_COMMON value", mappedEntry.getCommonName());
        assertEquals("TAX_CODE value", mappedEntry.getMnemonic());
        assertEquals(1001L, mappedEntry.getParentId());
        assertEquals(TaxonomyRank.FAMILY, mappedEntry.getRank());
        assertTrue(mappedEntry.getSynonyms().contains("SPTR_SYNONYM value"));
        assertTrue(mappedEntry.isHidden());
    }

    private StructType getTaxonomySchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.LongType, true);
        structType = structType.add("SPTR_COMMON", DataTypes.StringType, true);
        structType = structType.add("NCBI_COMMON", DataTypes.StringType, true);
        structType = structType.add("SPTR_SCIENTIFIC", DataTypes.StringType, true);
        structType = structType.add("NCBI_SCIENTIFIC", DataTypes.StringType, true);
        structType = structType.add("TAX_CODE", DataTypes.StringType, true);

        structType = structType.add("PARENT_ID", DataTypes.LongType, true);
        structType = structType.add("RANK", DataTypes.StringType, true);
        structType = structType.add("SPTR_SYNONYM", DataTypes.StringType, true);
        structType = structType.add("HIDDEN", DataTypes.LongType, true);
        return structType;
    }
}