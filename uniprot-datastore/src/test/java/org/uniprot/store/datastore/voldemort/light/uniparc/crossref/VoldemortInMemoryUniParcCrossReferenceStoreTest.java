package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;

class VoldemortInMemoryUniParcCrossReferenceStoreTest {

    private VoldemortInMemoryUniParcCrossReferenceStore store;

    @BeforeEach
    void setUp() {
        store = VoldemortInMemoryUniParcCrossReferenceStore.getInstance("cross-reference");
    }

    @AfterEach
    void tearDown() {
        store.truncate();
    }

    @Test
    void testSaveAndGetEntry() {
        // Create UniParcCrossReference using the builder
        UniParcCrossReference entry =
                new UniParcCrossReferenceBuilder()
                        .id("XREF1")
                        .database(UniParcDatabase.EG_BACTERIA)
                        .build();

        // Save the entry
        store.saveOrUpdateEntry(entry);

        // Retrieve the entry
        Optional<UniParcCrossReference> retrievedEntry = store.getEntry("XREF1");

        // Assert that the retrieved entry matches the saved entry
        assertTrue(retrievedEntry.isPresent());
        assertEquals("XREF1", retrievedEntry.get().getId());
        assertEquals(UniParcDatabase.EG_BACTERIA, retrievedEntry.get().getDatabase());
    }

    @Test
    void testSaveAndUpdateEntry() {
        // Create and save an entry
        UniParcCrossReference entry =
                new UniParcCrossReferenceBuilder()
                        .id("XREF2")
                        .database(UniParcDatabase.EMBL)
                        .build();
        store.saveOrUpdateEntry(entry);

        // Update the entry using builder
        entry =
                new UniParcCrossReferenceBuilder()
                        .id("XREF2")
                        .database(UniParcDatabase.EMBL_CON)
                        .build();
        store.saveOrUpdateEntry(entry);

        // Retrieve the updated entry
        Optional<UniParcCrossReference> retrievedEntry = store.getEntry("XREF2");

        // Assert that the retrieved entry reflects the update
        assertTrue(retrievedEntry.isPresent());
        assertEquals("XREF2", retrievedEntry.get().getId());
        assertEquals(UniParcDatabase.EMBL_CON, retrievedEntry.get().getDatabase());
    }

    @Test
    void testGetEntryMap() {
        // Create and save multiple entries
        UniParcCrossReference entry1 =
                new UniParcCrossReferenceBuilder()
                        .id("XREF3")
                        .database(UniParcDatabase.SWISSPROT)
                        .build();
        UniParcCrossReference entry2 =
                new UniParcCrossReferenceBuilder()
                        .id("XREF4")
                        .database(UniParcDatabase.TREMBL)
                        .build();
        store.saveOrUpdateEntry(entry1);
        store.saveOrUpdateEntry(entry2);

        // Retrieve entries as a map
        Map<String, UniParcCrossReference> entryMap =
                store.getEntryMap(Arrays.asList("XREF3", "XREF4"));

        // Assert that both entries are retrieved correctly
        assertEquals(2, entryMap.size());
        assertTrue(entryMap.containsKey("XREF3"));
        assertTrue(entryMap.containsKey("XREF4"));
        assertEquals(UniParcDatabase.SWISSPROT, entryMap.get("XREF3").getDatabase());
        assertEquals(UniParcDatabase.TREMBL, entryMap.get("XREF4").getDatabase());
    }

    @Test
    void testTruncate() {
        // Create and save an entry
        UniParcCrossReference entry =
                new UniParcCrossReferenceBuilder()
                        .id("XREF5")
                        .database(UniParcDatabase.EG_BACTERIA)
                        .build();
        store.saveOrUpdateEntry(entry);

        // Ensure the entry is saved
        assertTrue(store.getEntry("XREF5").isPresent());

        // Truncate the store
        store.truncate();

        // Ensure the store is empty
        assertFalse(store.getEntry("XREF5").isPresent());
    }
}
