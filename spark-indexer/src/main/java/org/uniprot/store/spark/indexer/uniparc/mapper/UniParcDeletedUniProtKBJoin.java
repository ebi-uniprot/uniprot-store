package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB;

import java.io.Serial;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

public class UniParcDeletedUniProtKBJoin
        implements Function<Tuple2<UniProtKBEntry, Optional<String>>, UniProtKBEntry> {

    @Serial private static final long serialVersionUID = 1028697335067985980L;

    @Override
    public UniProtKBEntry call(Tuple2<UniProtKBEntry, Optional<String>> tuple2) {
        UniProtKBEntry entry = tuple2._1;
        if (tuple2._2.isPresent() && isDeletedEntry(entry)) {
            UniProtKBEntryBuilder builder = UniProtKBEntryBuilder.from(entry);
            builder.extraAttributesAdd(UNIPARC_ID_ATTRIB, tuple2._2.get());
            entry = builder.build();
        } else {
            System.out.println("NOT JOINED KEY" + tuple2._1.getPrimaryAccession().getValue() + ":"+ tuple2._2.isPresent()+":" + isDeletedEntry(entry));
        }
        return entry;
    }

    private boolean isDeletedEntry(UniProtKBEntry entry) {
        return Utils.notNull(entry.getInactiveReason())
                && InactiveReasonType.DELETED.equals(
                        entry.getInactiveReason().getInactiveReasonType());
    }
}
