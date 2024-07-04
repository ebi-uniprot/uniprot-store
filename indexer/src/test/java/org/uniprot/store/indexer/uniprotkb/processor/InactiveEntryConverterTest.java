package org.uniprot.store.indexer.uniprotkb.processor;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 01/07/2021
 */
class InactiveEntryConverterTest {

    @Test
    void convertDeleted() {
        InactiveEntryConverter converter = new InactiveEntryConverter();
        InactiveUniProtEntry entry =
                new InactiveUniProtEntry(
                        "P12345", null, "DELETED", "UPI0001661588", Collections.emptyList(), null);
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertTrue(result.id.isEmpty());
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertNull(result.idInactive);
        assertEquals("DELETED", result.inactiveReason);
        assertFalse(result.active);
        assertEquals("UPI0001661588", result.deletedEntryUniParc);
    }

    @Test
    void convertDeletedWithId() {
        InactiveEntryConverter converter = new InactiveEntryConverter();
        InactiveUniProtEntry entry =
                new InactiveUniProtEntry(
                        "P12345", "ID", "DELETED", "UPI0001661588", Collections.emptyList(), null);
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals(1, result.id.size());
        assertTrue(result.id.contains("ID"));
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertEquals("ID", result.idInactive);
        assertEquals("DELETED", result.inactiveReason);
        assertFalse(result.active);
        assertEquals("UPI0001661588", result.deletedEntryUniParc);
    }

    @Test
    void convertMerged() {
        InactiveEntryConverter converter = new InactiveEntryConverter();
        List<String> mergedTo = List.of("P11111");
        InactiveUniProtEntry entry =
                new InactiveUniProtEntry("P12345", "ID1", "MERGED", null, mergedTo, null);
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals(1, result.id.size());
        assertTrue(result.id.contains("ID1"));
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertEquals("ID1", result.idInactive);
        assertEquals("MERGED:P11111", result.inactiveReason);
        assertFalse(result.active);
        assertNull(result.deletedEntryUniParc);
    }

    @Test
    void convertDeMerged() {
        InactiveEntryConverter converter = new InactiveEntryConverter();
        List<String> demergedTo = List.of("P11111", "P22222");
        InactiveUniProtEntry entry =
                new InactiveUniProtEntry("P12345", "ID1", "DEMERGED", null, demergedTo, null);
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals(1, result.id.size());
        assertTrue(result.id.contains("ID1"));
        assertTrue(Utils.nullOrEmpty(result.idDefault));
        assertEquals("DEMERGED:P11111,P22222", result.inactiveReason);
        assertFalse(result.active);
        assertTrue(result.content.isEmpty());
        assertNull(result.deletedEntryUniParc);
    }
}
