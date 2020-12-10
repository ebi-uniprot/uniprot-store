package org.uniprot.store.spark.indexer.suggest.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 25/11/2020
 */
class ProteomeToTaxonomyPairTest {

    @Test
    void call() throws Exception {
        String pId = "UP000008595";
        long taxonId = 9606L;
        Taxonomy taxonomy = new TaxonomyBuilder().taxonId(taxonId).build();
        ProteomeEntry entry = new ProteomeEntryBuilder().proteomeId(pId).taxonomy(taxonomy).build();

        ProteomeToTaxonomyPair mapper = new ProteomeToTaxonomyPair();
        Tuple2<String, String> result = mapper.call(new Tuple2<>(pId, entry));
        assertNotNull(result);
        assertEquals(String.valueOf(taxonId), result._1);
        assertEquals(pId, result._2);
    }
}
