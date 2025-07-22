package org.uniprot.store.spark.indexer.uniprot.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

public class GoogleProtLMEntryUpdater
        implements Function<Tuple2<UniProtKBEntry, UniProtKBEntry>, UniProtKBEntry> {
    @Override
    public UniProtKBEntry call(Tuple2<UniProtKBEntry, UniProtKBEntry> tuple) {
        UniProtKBEntry protLMEntry = tuple._1();
        UniProtKBEntry uniProtEntry = tuple._2();
        return UniProtKBEntryBuilder.from(protLMEntry)
                .uniProtId(uniProtEntry.getUniProtkbId())
                .build();
    }
}
