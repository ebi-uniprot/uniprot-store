package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

public class GoogleProtNLMEntryUpdater
        implements Function<Tuple2<UniProtKBEntry, UniProtKBEntry>, UniProtKBEntry>, Serializable {

    private static final long serialVersionUID = -3375925835880954913L;

    @Override
    public UniProtKBEntry call(Tuple2<UniProtKBEntry, UniProtKBEntry> tuple) {
        UniProtKBEntry protNLMEntry = tuple._1();
        UniProtKBEntry uniProtEntry = tuple._2();
        return UniProtKBEntryBuilder.from(protNLMEntry)
                .uniProtId(uniProtEntry.getUniProtkbId())
                .build();
    }
}
