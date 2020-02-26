package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.builder.UniParcEntryBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-02-20
 */
class UniParcTaxonomyMapperTest {

    @Test
    void testTaxonomyJoin() throws Exception {
        UniParcTaxonomyMapper mapper = new UniParcTaxonomyMapper();
        UniParcEntry entry =
                new UniParcEntryBuilder()
                        .uniParcId("uniParcIdValue")
                        .taxonomiesAdd(new TaxonomyEntryBuilder().taxonId(10).build())
                        .taxonomiesAdd(new TaxonomyEntryBuilder().taxonId(11).build())
                        .build();
        Iterator<Tuple2<String, String>> result = mapper.call(entry);
        assertNotNull(result);
        assertTrue(result.hasNext());
        Tuple2<String, String> item1 = result.next();
        assertEquals("10", item1._1);
        assertEquals("uniParcIdValue", item1._2);

        assertTrue(result.hasNext());
        Tuple2<String, String> item2 = result.next();
        assertEquals("11", item2._1);
        assertEquals("uniParcIdValue", item2._2);
    }

    @Test
    void testTaxonomyJoinEmptyTaxonomy() throws Exception {
        UniParcTaxonomyMapper mapper = new UniParcTaxonomyMapper();
        UniParcEntry entry = new UniParcEntryBuilder().uniParcId("uniParcIdValue").build();
        Iterator<Tuple2<String, String>> result = mapper.call(entry);
        assertNotNull(result);
        assertFalse(result.hasNext());
    }
}
