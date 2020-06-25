package org.uniprot.store.spark.indexer.uniprot.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

/**
 * This class is Responsible to Map Optional<UniParcId> into UniProtKBEntry extraAttributes
 * @author lgonzales
 * @since 24/06/2020
 */
public class UniParcMapper
        implements Function<Tuple2<UniProtKBEntry, Optional<String>>, UniProtKBEntry> {

    private static final long serialVersionUID = -2921487786031600491L;

    @Override
    public UniProtKBEntry call(Tuple2<UniProtKBEntry, Optional<String>> tuple) throws Exception {
        UniProtKBEntry entry = tuple._1;
        if (tuple._2.isPresent()) {
            String uniParcId = tuple._2.get();
            entry =
                    UniProtKBEntryBuilder.from(entry)
                            .extraAttributesAdd(UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB, uniParcId)
                            .build();
        }
        return entry;
    }
}
