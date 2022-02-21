package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashSet;

import org.junit.jupiter.api.Test;

/**
 * @author sahmad
 * @created 08/02/2022
 */
class CombineMappedProteinAccessionSetsTest {

    @Test
    void testCombine() throws Exception {
        CombineMappedProteinAccessionSets combineFunction = new CombineMappedProteinAccessionSets();
        MappedProteinAccession mpa1 = MappedProteinAccession.builder().build();
        MappedProteinAccession mpa2 =
                MappedProteinAccession.builder().proteinAccession("P12345").build();
        MappedProteinAccession mpa3 =
                MappedProteinAccession.builder().proteinAccession("Q12345").build();
        MappedProteinAccession mpa4 =
                MappedProteinAccession.builder().proteinAccession("P22345").build();
        HashSet<MappedProteinAccession> input1 = new HashSet<>();
        input1.add(mpa1);
        input1.add(mpa2);
        input1.add(mpa3);
        HashSet<MappedProteinAccession> input2 = new HashSet<>();
        input2.add(mpa3);
        input2.add(mpa4);
        HashSet<MappedProteinAccession> result = combineFunction.call(input1, input2);
        assertNotNull(result);
        assertEquals(4, result.size());
    }

    @Test
    void testCombineWithOneEmpty() throws Exception {
        CombineMappedProteinAccessionSets combineFunction = new CombineMappedProteinAccessionSets();
        MappedProteinAccession mpa1 = MappedProteinAccession.builder().build();
        MappedProteinAccession mpa2 =
                MappedProteinAccession.builder().proteinAccession("P12345").build();
        MappedProteinAccession mpa3 =
                MappedProteinAccession.builder().proteinAccession("Q12345").build();
        HashSet<MappedProteinAccession> input1 = new HashSet<>();
        input1.add(mpa1);
        input1.add(mpa2);
        input1.add(mpa3);
        HashSet<MappedProteinAccession> input2 = new HashSet<>();
        HashSet<MappedProteinAccession> result = combineFunction.call(input1, input2);
        assertNotNull(result);
        assertEquals(3, result.size());
    }
}
