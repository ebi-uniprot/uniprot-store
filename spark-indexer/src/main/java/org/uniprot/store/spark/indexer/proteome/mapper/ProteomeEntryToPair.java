package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 21/08/2020
 */
public class ProteomeEntryToPair implements PairFunction<ProteomeEntry, String, ProteomeEntry> {

    private static final long serialVersionUID = -2624019074018751390L;

    @Override
    public Tuple2<String, ProteomeEntry> call(ProteomeEntry proteomeEntry) throws Exception {
        return new Tuple2<>(proteomeEntry.getId().getValue(), proteomeEntry);
    }
}
