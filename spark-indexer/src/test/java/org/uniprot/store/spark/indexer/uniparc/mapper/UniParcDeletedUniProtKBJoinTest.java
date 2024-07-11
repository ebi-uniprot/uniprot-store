package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder.*;

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
        UniProtKBEntry entry = getObsoleteUniProtKBEntry(InactiveReasonType.DELETED);
        Tuple2<UniProtKBEntry, Optional<String>> tuple2 =
                new Tuple2<>(entry, Optional.of(uniParcId));
        UniProtKBEntry result = join.call(tuple2);
        assertNotNull(result);
        assertEquals(uniParcId, result.getExtraAttributeValue(UNIPARC_ID_ATTRIB));
    }

    @Test
    void doesNotMapUniParcIdForMergedEntries() {
        UniParcDeletedUniProtKBJoin join = new UniParcDeletedUniProtKBJoin();
        String uniParcId = "UPI00000E8551";
        UniProtKBEntry entry = getObsoleteUniProtKBEntry(InactiveReasonType.MERGED);
        Tuple2<UniProtKBEntry, Optional<String>> tuple2 =
                new Tuple2<>(entry, Optional.of(uniParcId));
        UniProtKBEntry result = join.call(tuple2);
        assertNotNull(result);
        assertNull(result.getExtraAttributeValue(UNIPARC_ID_ATTRIB));
    }

    @Test
    void doesNotMapForEmptyUniParcId() {
        UniParcDeletedUniProtKBJoin join = new UniParcDeletedUniProtKBJoin();
        UniProtKBEntry entry = getObsoleteUniProtKBEntry(InactiveReasonType.DELETED);
        Tuple2<UniProtKBEntry, Optional<String>> tuple2 = new Tuple2<>(entry, Optional.empty());
        UniProtKBEntry result = join.call(tuple2);
        assertNotNull(result);
        assertNull(result.getExtraAttributeValue(UNIPARC_ID_ATTRIB));
    }

    private static UniProtKBEntry getObsoleteUniProtKBEntry(InactiveReasonType inactiveReasonType) {
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder().type(inactiveReasonType).build();
        return new UniProtKBEntryBuilder("AC1", "ID1", inactiveReason).build();
    }
}
