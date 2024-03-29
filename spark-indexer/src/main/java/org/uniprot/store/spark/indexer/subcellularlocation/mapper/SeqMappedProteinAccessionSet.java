package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.function.Function2;

/**
 * @author sahmad
 * @created 07/02/2022
 */
public class SeqMappedProteinAccessionSet
        implements Function2<
                HashSet<MappedProteinAccession>,
                Iterable<MappedProteinAccession>,
                HashSet<MappedProteinAccession>> {
    @Override
    public HashSet<MappedProteinAccession> call(
            HashSet<MappedProteinAccession> joined, Iterable<MappedProteinAccession> input)
            throws Exception {
        List<MappedProteinAccession> inputList = new ArrayList<>();
        input.forEach(inputList::add);
        joined.addAll(inputList);
        return joined;
    }
}
