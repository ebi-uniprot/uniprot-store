package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.Strain;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TaxonomyStrainsRowMapperTest {

    @Test
    void mapStrainScientificName() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // TAX_ID
        values.add(new BigDecimal(1)); // STRAIN_ID
        values.add("nameValue"); // NAME
        values.add("scientific name"); // NAME_CLASS
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyStrainSchema());

        TaxonomyStrainsRowMapper mapper = new TaxonomyStrainsRowMapper();
        Tuple2<String, Strain> result = mapper.call(row);
        assertNotNull(result);
        assertEquals("1000", result._1);
        assertNotNull(result._2);
        assertEquals(1, result._2.getId());
        assertEquals("nameValue", result._2.getName());
        assertEquals(Strain.StrainNameClass.scientific_name,result._2.getNameClass());
    }

    @Test
    void mapStrainSynonym() throws Exception {
        List<Object> values = new ArrayList<>();
        values.add(new BigDecimal(1000)); // TAX_ID
        values.add(new BigDecimal(1)); // STRAIN_ID
        values.add("synonymValue"); // NAME
        values.add("synonym"); // NAME_CLASS
        Row row = new GenericRowWithSchema(values.toArray(), getTaxonomyStrainSchema());

        TaxonomyStrainsRowMapper mapper = new TaxonomyStrainsRowMapper();
        Tuple2<String, Strain> result = mapper.call(row);
        assertNotNull(result);
        assertEquals("1000", result._1);
        assertNotNull(result._2);
        assertEquals(1, result._2.getId());
        assertEquals("synonymValue", result._2.getName());
        assertEquals(Strain.StrainNameClass.synonym,result._2.getNameClass());
    }

    private StructType getTaxonomyStrainSchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.LongType, true);
        structType = structType.add("STRAIN_ID", DataTypes.LongType, true);
        structType = structType.add("NAME", DataTypes.StringType, true);
        structType = structType.add("NAME_CLASS", DataTypes.StringType, true);
        return structType;
    }
}