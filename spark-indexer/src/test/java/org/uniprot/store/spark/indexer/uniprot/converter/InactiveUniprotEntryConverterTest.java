package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.EntryInactiveReason;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
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
                new EntryInactiveReasonBuilder().type(InactiveReasonType.DELETED).build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("P12345", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertNull(result.id);
        assertNull(result.idDefault);
        assertEquals("DELETED", result.inactiveReason);
        assertFalse(result.active);
        assertTrue(result.content.contains("P12345"));
    }

    @Test
    void convertDeletedWithId() {
        InactiveUniprotEntryConverter converter = new InactiveUniprotEntryConverter();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder().type(InactiveReasonType.DELETED).build();
        UniProtKBEntry entry = new UniProtKBEntryBuilder("P12345", "ID", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals("ID", result.id);
        assertEquals("ID", result.idDefault);
        assertEquals("DELETED", result.inactiveReason);
        assertFalse(result.active);
        assertTrue(result.content.contains("P12345"));
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
        assertEquals("ID1", result.id);
        assertEquals("ID1", result.idDefault);
        assertEquals("MERGED:P11111", result.inactiveReason);
        assertFalse(result.active);
        assertTrue(result.content.isEmpty());
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
        assertEquals("ID1", result.id);
        assertNull(result.idDefault);
        assertEquals("DEMERGED:P11111,P22222", result.inactiveReason);
        assertFalse(result.active);
        assertTrue(result.content.isEmpty());
    }
}
