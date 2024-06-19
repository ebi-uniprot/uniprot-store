package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.*;
import org.uniprot.core.uniprotkb.impl.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

class UniParcDeletedUniProtKBJoinTest {

    @Test
    void mapUniParcIdForDeletedEntries() {
        UniParcDeletedUniProtKBJoin join = new UniParcDeletedUniProtKBJoin();
        String uniParcId = "UPI00000E8551";
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder().type(InactiveReasonType.DELETED).build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("AC1", "ID1", inactiveReason).build();
        Tuple2<UniProtKBEntry, Optional<String>> tuple2 =
                new Tuple2<>(entry, Optional.of(uniParcId));
        UniProtKBEntry result = join.call(tuple2);
        assertNotNull(result);
        assertEquals(
                uniParcId, result.getExtraAttributeValue(UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB));
    }

    @Test
    void doesNotMapUniParcIdForMergedEntries() {
        UniParcDeletedUniProtKBJoin join = new UniParcDeletedUniProtKBJoin();
        String uniParcId = "UPI00000E8551";
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder().type(InactiveReasonType.MERGED).build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("AC1", "ID1", inactiveReason).build();
        Tuple2<UniProtKBEntry, Optional<String>> tuple2 =
                new Tuple2<>(entry, Optional.of(uniParcId));
        UniProtKBEntry result = join.call(tuple2);
        assertNotNull(result);
        assertNull(result.getExtraAttributeValue(UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB));
    }

    @Test
    void doesNotMapForEmptyUniParcId() {
        UniParcDeletedUniProtKBJoin join = new UniParcDeletedUniProtKBJoin();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder().type(InactiveReasonType.DELETED).build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("AC1", "ID1", inactiveReason).build();
        Tuple2<UniProtKBEntry, Optional<String>> tuple2 = new Tuple2<>(entry, Optional.empty());
        UniProtKBEntry result = join.call(tuple2);
        assertNotNull(result);
        assertNull(result.getExtraAttributeValue(UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB));
    }
}
