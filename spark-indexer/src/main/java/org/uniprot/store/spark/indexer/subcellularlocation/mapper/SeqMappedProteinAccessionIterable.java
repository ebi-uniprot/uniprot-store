package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sahmad
 * @created 07/02/2022
 */
public class SeqMappedProteinAccessionIterable implements Function2<Iterable<MappedProteinAccession>,
        MappedProteinAccession, Iterable<MappedProteinAccession>> {


    @Override
    public Iterable<MappedProteinAccession> call(Iterable<MappedProteinAccession> iterable, MappedProteinAccession input) throws Exception {
        List<MappedProteinAccession> result = new ArrayList<>();
        iterable.forEach(result::add);
        result.add(input);
        return result;
    }
}
