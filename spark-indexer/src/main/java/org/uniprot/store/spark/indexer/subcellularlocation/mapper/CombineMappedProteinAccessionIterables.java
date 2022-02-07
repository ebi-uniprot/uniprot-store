package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sahmad
 * @created 07/02/2022
 */
public class CombineMappedProteinAccessionIterables implements Function2<Iterable<MappedProteinAccession>,
        Iterable<MappedProteinAccession>, Iterable<MappedProteinAccession>> {
    @Override
    public Iterable<MappedProteinAccession> call(Iterable<MappedProteinAccession> input1,
                                                 Iterable<MappedProteinAccession> input2) throws Exception {
        List<MappedProteinAccession> result = new ArrayList<>();
        input1.forEach(result::add);
        input2.forEach(result::add);
        return result;
    }
}
