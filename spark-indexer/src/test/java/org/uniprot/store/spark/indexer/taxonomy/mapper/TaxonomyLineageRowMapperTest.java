package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyLineageRowMapperTest {

    @Test
    void testTaxonomyLineageRowMapperRoot() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add("|1"); // LINEAGE_ID
        values.add("|root"); // LINEAGE_NAME
        values.add("| "); // LINEAGE_COMMON
        values.add("|no rank"); // LINEAGE_RANK
        values.add("|1"); // LINEAGE_HIDDEN

        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyLineageSchema());

        TaxonomyLineageRowMapper taxonomyRowMapper = new TaxonomyLineageRowMapper(false);
        Tuple2<String, List<TaxonomyLineage>> result = taxonomyRowMapper.call(row);

        assertNotNull(result);

        assertEquals("1", result._1);
        List<TaxonomyLineage> mappedLineage = result._2;
        assertNotNull(mappedLineage);
        assertTrue(mappedLineage.isEmpty());
    }

    @Test
    void testTaxonomyLineageRowMapper() throws Exception {
        List<Object> values = getLineageValues();

        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyLineageSchema());

        TaxonomyLineageRowMapper taxonomyRowMapper = new TaxonomyLineageRowMapper(false);
        Tuple2<String, List<TaxonomyLineage>> result = taxonomyRowMapper.call(row);

        assertNotNull(result);

        assertEquals("6", result._1);
        List<TaxonomyLineage> mappedLineage = result._2;
        assertNotNull(mappedLineage);
        assertEquals(6, mappedLineage.size());

        TaxonomyLineage cellularOrganism = mappedLineage.get(0);
        assertEquals(131567L, cellularOrganism.getTaxonId());
        assertEquals("cellular organisms", cellularOrganism.getScientificName());
        assertEquals("", cellularOrganism.getCommonName());
        assertEquals(TaxonomyRank.NO_RANK, cellularOrganism.getRank());
        assertTrue(cellularOrganism.isHidden());

        TaxonomyLineage bacteria = mappedLineage.get(1);
        assertEquals(2L, bacteria.getTaxonId());
        assertEquals("Bacteria", bacteria.getScientificName());
        assertEquals("eubacteria", bacteria.getCommonName());
        assertEquals(TaxonomyRank.SUPERKINGDOM, bacteria.getRank());
        assertFalse(bacteria.isHidden());

        TaxonomyLineage xantho = mappedLineage.get(5);
        assertEquals(335928L, xantho.getTaxonId());
        assertEquals("Xanthobacteraceae", xantho.getScientificName());
        assertTrue(xantho.getCommonName().isEmpty());
        assertEquals(TaxonomyRank.FAMILY, xantho.getRank());
        assertFalse(xantho.isHidden());
    }

    @Test
    void testTaxonomyLineageRowMapperIncludeOrganism() throws Exception {
        List<Object> values = getLineageValues();

        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyLineageSchema());

        TaxonomyLineageRowMapper taxonomyRowMapper = new TaxonomyLineageRowMapper(true);
        Tuple2<String, List<TaxonomyLineage>> result = taxonomyRowMapper.call(row);

        assertNotNull(result);

        assertEquals("6", result._1);
        List<TaxonomyLineage> mappedLineage = result._2;
        assertNotNull(mappedLineage);
        assertEquals(7, mappedLineage.size());

        TaxonomyLineage cellularOrganism = mappedLineage.get(0);
        assertEquals(131567L, cellularOrganism.getTaxonId());
        assertEquals("cellular organisms", cellularOrganism.getScientificName());
        assertEquals("", cellularOrganism.getCommonName());
        assertEquals(TaxonomyRank.NO_RANK, cellularOrganism.getRank());
        assertTrue(cellularOrganism.isHidden());

        TaxonomyLineage azorhizobium = mappedLineage.get(6);
        assertEquals(6L, azorhizobium.getTaxonId());
        assertEquals("Azorhizobium", azorhizobium.getScientificName());
        assertTrue(azorhizobium.getCommonName().isEmpty());
        assertEquals(TaxonomyRank.GENUS, azorhizobium.getRank());
        assertFalse(azorhizobium.isHidden());
    }

    private StructType getTaxonomyLineageSchema() {
        StructType structType = new StructType();
        structType = structType.add("LINEAGE_ID", DataTypes.StringType, true);
        structType = structType.add("LINEAGE_NAME", DataTypes.StringType, true);
        structType = structType.add("LINEAGE_COMMON", DataTypes.StringType, true);
        structType = structType.add("LINEAGE_RANK", DataTypes.StringType, true);
        structType = structType.add("LINEAGE_HIDDEN", DataTypes.StringType, true);
        return structType;
    }

    @NotNull
    private List<Object> getLineageValues() {
        List<Object> values = new ArrayList<>();
        values.add("|6|335928|356|28211|1224|2|131567|1"); // LINEAGE_ID
        values.add(
                "|Azorhizobium|Xanthobacteraceae|Rhizobiales|Alphaproteobacteria|Proteobacteria|Bacteria|cellular organisms|root"); // LINEAGE_NAME
        values.add("| | |rhizobacteria| | |eubacteria| | "); // LINEAGE_COMMON
        values.add("|genus|family|order|class|phylum|superkingdom|no rank|no rank"); // LINEAGE_RANK
        values.add("|0|0|0|0|0|0|1|1"); // LINEAGE_HIDDEN
        return values;
    }
}
