package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.function.Function2;

import java.util.HashSet;

/**
 * @author sahmad
 * @created 07/02/2022
 */
public class CombineMappedProteinAccessionSets implements Function2<HashSet<MappedProteinAccession>,
        HashSet<MappedProteinAccession>, HashSet<MappedProteinAccession>> {
    @Override
    public HashSet<MappedProteinAccession> call(HashSet<MappedProteinAccession> input1,
                                                HashSet<MappedProteinAccession> input2) throws Exception {
        input1.addAll(input2);
        return input1;
    }
}
