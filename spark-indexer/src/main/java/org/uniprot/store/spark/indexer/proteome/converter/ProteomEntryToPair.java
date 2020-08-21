package org.uniprot.store.spark.indexer.proteome.converter;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 21/08/2020
 */
public class ProteomEntryToPair implements PairFunction<ProteomeEntry, String, ProteomeEntry> {

    @Override
    public Tuple2<String, ProteomeEntry> call(ProteomeEntry proteomeEntry) throws Exception {
        return new Tuple2<>(proteomeEntry.getId().getValue(), proteomeEntry);
    }
}
