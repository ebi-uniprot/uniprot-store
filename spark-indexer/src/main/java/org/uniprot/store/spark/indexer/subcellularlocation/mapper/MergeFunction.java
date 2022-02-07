package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.function.Function2;

import java.util.HashSet;

/**
 * @author sahmad
 * @created 07/02/2022
 */
public class MergeFunction implements Function2<HashSet<MappedProteinAccession>, HashSet<MappedProteinAccession>, HashSet<MappedProteinAccession>> {
    @Override
    public HashSet<MappedProteinAccession> call(HashSet<MappedProteinAccession> v1, HashSet<MappedProteinAccession> v2) throws Exception {
        v1.addAll(v2);
        return v1;
    }
}
