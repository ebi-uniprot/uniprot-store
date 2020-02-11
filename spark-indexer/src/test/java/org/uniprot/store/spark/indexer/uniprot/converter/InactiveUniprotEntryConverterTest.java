package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.EntryInactiveReason;
import org.uniprot.core.uniprot.InactiveReasonType;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.builder.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;
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
        UniProtEntry entry = new UniProtEntryBuilder("P12345", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertNull(result.id);
        assertEquals("DELETED", result.inactiveReason);
        assertFalse(result.active);
    }

    @Test
    void convertMerged() {
        InactiveUniprotEntryConverter converter = new InactiveUniprotEntryConverter();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.MERGED)
                        .addMergeDemergeTo("P11111")
                        .build();
        UniProtEntry entry = new UniProtEntryBuilder("P12345", "ID1", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals("ID1", result.id);
        assertEquals("MERGED:P11111", result.inactiveReason);
        assertFalse(result.active);
    }

    @Test
    void convertDeMerged() {
        InactiveUniprotEntryConverter converter = new InactiveUniprotEntryConverter();
        EntryInactiveReason inactiveReason =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.DEMERGED)
                        .addMergeDemergeTo("P11111")
                        .addMergeDemergeTo("P22222")
                        .build();
        UniProtEntry entry = new UniProtEntryBuilder("P12345", "ID1", inactiveReason).build();
        UniProtDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("P12345", result.accession);
        assertEquals("ID1", result.id);
        assertEquals("DEMERGED:P11111,P22222", result.inactiveReason);
        assertFalse(result.active);
    }
}
