package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.DeletedReason;
import org.uniprot.core.uniprotkb.EntryInactiveReason;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2020-02-10
 */
class InactiveUniprotEntryConverterTest {

    @Test
    void convertDeleted() {
        InactiveUniprotEntryConverter converter = new InactiveUniprotEntryConverter();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.DELETED)
                        .deletedReason(DeletedReason.SOURCE_DELETION_EMBL)
                        .build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("P12345", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertTrue(Utils.nullOrEmpty(result.id));
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertNull(result.idInactive);
        assertEquals("DELETED:SOURCE_DELETION_EMBL", result.inactiveReason);
        assertFalse(result.active);
    }

    @Test
    void convertDeletedWithIdAndWithoutDeleteReason() {
        InactiveUniprotEntryConverter converter = new InactiveUniprotEntryConverter();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder().type(InactiveReasonType.DELETED).build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("P12345", "ID", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals(1, result.id.size());
        assertTrue(result.id.contains("ID"));
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertEquals("ID", result.idInactive);
        assertEquals("DELETED", result.inactiveReason);
        assertFalse(result.active);
    }

    @Test
    void convertMerged() {
        InactiveUniprotEntryConverter converter = new InactiveUniprotEntryConverter();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.MERGED)
                        .mergeDemergeTosAdd("P11111")
                        .build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("P12345", "ID1", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals(1, result.id.size());
        assertTrue(result.id.contains("ID1"));
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertEquals("ID1", result.idInactive);
        assertEquals("MERGED:P11111", result.inactiveReason);
        assertFalse(result.active);
    }

    @Test
    void convertDeMerged() {
        InactiveUniprotEntryConverter converter = new InactiveUniprotEntryConverter();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.DEMERGED)
                        .mergeDemergeTosAdd("P11111")
                        .mergeDemergeTosAdd("P22222")
                        .build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("P12345", "ID1", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals(1, result.id.size());
        assertTrue(result.id.contains("ID1"));
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertEquals("DEMERGED:P11111,P22222", result.inactiveReason);
        assertFalse(result.active);
        assertTrue(result.content.isEmpty());
    }
}
