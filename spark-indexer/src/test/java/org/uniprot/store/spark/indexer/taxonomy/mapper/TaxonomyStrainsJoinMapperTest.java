package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyStrain;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.Strain;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TaxonomyStrainsJoinMapperTest {

    @Test
    void mapStrain() throws Exception {
        TaxonomyStrainsJoinMapper mapper = new TaxonomyStrainsJoinMapper();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        List<Strain> strains = new ArrayList<>();
        strains.add(new Strain(1L, Strain.StrainNameClass.scientific_name, "name1"));
        strains.add(new Strain(2L, Strain.StrainNameClass.scientific_name, "name2"));
        Tuple2<TaxonomyEntry, Optional<Iterable<Strain>>> tuple = new Tuple2<>(entry, Optional.of(strains));
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getStrains());
        assertEquals(2, result.getStrains().size());
        TaxonomyStrain strain1 = result.getStrains().get(0);
        assertEquals("name1", strain1.getName());
        assertTrue(strain1.getSynonyms().isEmpty());

        TaxonomyStrain strain2 = result.getStrains().get(1);
        assertEquals("name2", strain2.getName());
        assertTrue(strain2.getSynonyms().isEmpty());
    }

    @Test
    void mapStrainWithSynonym() throws Exception {
        TaxonomyStrainsJoinMapper mapper = new TaxonomyStrainsJoinMapper();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        List<Strain> strains = new ArrayList<>();
        strains.add(new Strain(1L, Strain.StrainNameClass.scientific_name, "name1"));
        strains.add(new Strain(2L, Strain.StrainNameClass.scientific_name, "name2"));
        strains.add(new Strain(2L, Strain.StrainNameClass.synonym, "synonym2"));
        strains.add(new Strain(1L, Strain.StrainNameClass.synonym, "synonym1"));
        strains.add(new Strain(2L, Strain.StrainNameClass.synonym, "synonym22"));
        Tuple2<TaxonomyEntry, Optional<Iterable<Strain>>> tuple = new Tuple2<>(entry, Optional.of(strains));
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getStrains());
        assertEquals(2, result.getStrains().size());
        TaxonomyStrain strain1 = result.getStrains().get(0);
        assertEquals("name1", strain1.getName());
        assertEquals(1, strain1.getSynonyms().size());
        assertTrue(strain1.getSynonyms().contains("synonym1"));

        TaxonomyStrain strain2 = result.getStrains().get(1);
        assertEquals("name2", strain2.getName());
        assertEquals(2, strain2.getSynonyms().size());
        assertTrue(strain2.getSynonyms().contains("synonym2"));
        assertTrue(strain2.getSynonyms().contains("synonym22"));
    }

    @Test
    void mapStrainEmpty() throws Exception {
        TaxonomyStrainsJoinMapper mapper = new TaxonomyStrainsJoinMapper();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        Tuple2<TaxonomyEntry, Optional<Iterable<Strain>>> tuple = new Tuple2<>(entry, Optional.empty());
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(entry, result);
    }
}