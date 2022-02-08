package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author sahmad
 * @created 08/02/2022
 */
class SeqMappedProteinAccessionSetTest {

    @Test
    void testMergeIterable() throws Exception {
        SeqMappedProteinAccessionSet merger = new SeqMappedProteinAccessionSet();
        HashSet<MappedProteinAccession> bag = new HashSet<>();
        List<MappedProteinAccession> iterable = new ArrayList<>();
        MappedProteinAccession mpa1 =
                MappedProteinAccession.builder().proteinAccession("P12345").build();
        MappedProteinAccession mpa2 =
                MappedProteinAccession.builder().proteinAccession("Q12345").build();
        iterable.add(mpa1);
        iterable.add(mpa2);
        HashSet<MappedProteinAccession> result = merger.call(bag, iterable);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
    }
}
