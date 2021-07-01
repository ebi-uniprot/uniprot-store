package org.uniprot.store.indexer.uniprotkb.processor;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
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
                new InactiveUniProtEntry("P12345", null, "DELETED", Collections.emptyList());
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
        InactiveEntryConverter converter = new InactiveEntryConverter();
        InactiveUniProtEntry entry =
                new InactiveUniProtEntry("P12345", "ID", "DELETED", Collections.emptyList());
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
        InactiveEntryConverter converter = new InactiveEntryConverter();
        List<String> mergedTo = List.of("P11111");
        InactiveUniProtEntry entry = new InactiveUniProtEntry("P12345", "ID1", "MERGED", mergedTo);
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
        InactiveEntryConverter converter = new InactiveEntryConverter();
        List<String> demergedTo = List.of("P11111", "P22222");
        InactiveUniProtEntry entry =
                new InactiveUniProtEntry("P12345", "ID1", "DEMERGED", demergedTo);
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
