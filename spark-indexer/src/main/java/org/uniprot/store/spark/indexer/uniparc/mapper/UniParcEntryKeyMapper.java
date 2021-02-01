package org.uniprot.store.spark.indexer.uniparc.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniparc.UniParcEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 22/01/2021
 */
public class UniParcEntryKeyMapper implements PairFunction<UniParcEntry, String, UniParcEntry> {
    private static final long serialVersionUID = 8388548616095686101L;

    @Override
    public Tuple2<String, UniParcEntry> call(UniParcEntry entry) throws Exception {
        return new Tuple2<>(entry.getUniParcId().getValue(), entry);
    }
}
