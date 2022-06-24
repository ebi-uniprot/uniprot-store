package org.uniprot.store.spark.indexer.uniparc.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;

import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import scala.Tuple2;

import java.util.List;

/**
 * @author lgonzales
 * @since 22/01/2021
 */
public class UniParcEntryKeyMapper implements PairFunction<UniParcEntry, String, UniParcEntry> {
    private static final long serialVersionUID = 8388548616095686101L;

    @Override
    public Tuple2<String, UniParcEntry> call(UniParcEntry entry) throws Exception {
        if (entry.getUniParcCrossReferences().size() > 10000){
            UniParcEntryBuilder builder = UniParcEntryBuilder.from(entry);
            builder.uniParcCrossReferencesSet(entry.getUniParcCrossReferences().subList(0, 10000));
            entry = builder.build();
        }
        return new Tuple2<>(entry.getUniParcId().getValue(), entry);
    }
}
