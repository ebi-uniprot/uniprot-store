package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

class GoogleProtNLMEntryUpdaterTest {

    private final GoogleProtNLMEntryUpdater updater = new GoogleProtNLMEntryUpdater();

    @Test
    void testUpdatesUniProtIdFromUniProtEntry() {
        // given
        String accession = "P12345";
        UniProtKBEntry protNLMEntry =
                new UniProtKBEntryBuilder(accession, "PLACEHOLDER", UniProtKBEntryType.TREMBL)
                        .build();

        UniProtKBEntry uniProtEntry =
                new UniProtKBEntryBuilder(accession, "New Value", UniProtKBEntryType.TREMBL)
                        .build();

        Tuple2<UniProtKBEntry, UniProtKBEntry> input = new Tuple2<>(protNLMEntry, uniProtEntry);

        // when
        UniProtKBEntry updatedEntry = updater.call(input);

        // then
        assertEquals("New Value", updatedEntry.getUniProtkbId().getValue());
    }

    @Test
    void testWhenIdIsSameItStillBuildsNewEntry() {
        UniProtKBEntry original =
                new UniProtKBEntryBuilder("P12345", "PLACEHOLDER", UniProtKBEntryType.TREMBL)
                        .build();

        Tuple2<UniProtKBEntry, UniProtKBEntry> input = new Tuple2<>(original, original);

        UniProtKBEntry updated = updater.call(input);

        assertEquals("PLACEHOLDER", updated.getUniProtkbId().getValue());
        assertNotSame(original, updated); // ensure it's a new object
    }
}
