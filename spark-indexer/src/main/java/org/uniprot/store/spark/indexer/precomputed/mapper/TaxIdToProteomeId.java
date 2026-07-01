package org.uniprot.store.spark.indexer.precomputed.mapper;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;

import scala.Tuple2;

public class TaxIdToProteomeId
        implements Serializable, PairFunction<ProteomeEntry, Integer, String> {
    @Override
    public Tuple2<Integer, String> call(ProteomeEntry proteomeEntry) throws Exception {
        return new Tuple2<>(
                (int) proteomeEntry.getTaxonomy().getTaxonId(), proteomeEntry.getId().getValue());
    }
}
