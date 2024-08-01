package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.util.Pair;
import org.uniprot.core.util.PairImpl;

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
        String key = "UP1";
        UniParcCrossReference xref =
                new UniParcCrossReferenceBuilder()
                        .id("XREF1")
                        .database(UniParcDatabase.EG_BACTERIA)
                        .build();

        Pair<String, List<UniParcCrossReference>> entry = new PairImpl<>(key, List.of(xref));
        // Save the entry
        store.saveOrUpdateEntry(entry);

        // Retrieve the entry
        Optional<Pair<String, List<UniParcCrossReference>>> retrievedEntry = store.getEntry(key);

        // Assert that the retrieved entry matches the saved entry
        assertTrue(retrievedEntry.isPresent());
        assertEquals(key, retrievedEntry.get().getKey());
        assertEquals(entry, retrievedEntry.get());
    }

    @Test
    void testSaveAndUpdateEntry() {
        // Create and save an entry
        String key = "UP2";
        UniParcCrossReference xref =
                new UniParcCrossReferenceBuilder()
                        .id("XREF2")
                        .database(UniParcDatabase.EMBL)
                        .build();
        Pair<String, List<UniParcCrossReference>> entry = new PairImpl<>(key, List.of(xref));
        store.saveOrUpdateEntry(entry);

        // Update the entry using builder
        UniParcCrossReference xrefUpdated =
                new UniParcCrossReferenceBuilder()
                        .id("XREF2")
                        .database(UniParcDatabase.EMBL_CON)
                        .build();
        Pair<String, List<UniParcCrossReference>> entryUpdated =
                new PairImpl<>(key, List.of(xrefUpdated));
        store.saveOrUpdateEntry(entryUpdated);

        // Retrieve the updated entry
        Optional<Pair<String, List<UniParcCrossReference>>> retrievedEntry = store.getEntry(key);

        // Assert that the retrieved entry reflects the update
        assertTrue(retrievedEntry.isPresent());
        assertEquals(key, retrievedEntry.get().getKey());
        assertEquals(entryUpdated, retrievedEntry.get());
    }

    @Test
    void testGetEntryMap() {
        // Create and save multiple entries
        String key3 = "UP3";
        UniParcCrossReference xref3 =
                new UniParcCrossReferenceBuilder()
                        .id("XREF3")
                        .database(UniParcDatabase.SWISSPROT)
                        .build();
        Pair<String, List<UniParcCrossReference>> entry3 = new PairImpl<>(key3, List.of(xref3));

        String key4 = "UP4";
        UniParcCrossReference xref4 =
                new UniParcCrossReferenceBuilder()
                        .id("XREF4")
                        .database(UniParcDatabase.TREMBL)
                        .build();
        Pair<String, List<UniParcCrossReference>> entry4 = new PairImpl<>(key4, List.of(xref4));
        store.saveOrUpdateEntry(entry3);
        store.saveOrUpdateEntry(entry4);

        // Retrieve entries as a map
        Map<String, Pair<String, List<UniParcCrossReference>>> entryMap =
                store.getEntryMap(Arrays.asList(key3, key4));

        // Assert that both entries are retrieved correctly
        assertEquals(2, entryMap.size());
        assertTrue(entryMap.containsKey(key3));
        assertTrue(entryMap.containsKey(key4));
        assertEquals(entry3, entryMap.get(key3));
        assertEquals(entry4, entryMap.get(key4));
    }

    @Test
    void testTruncate() {
        // Create and save an entry
        String key5 = "UP5";
        UniParcCrossReference xref5 =
                new UniParcCrossReferenceBuilder()
                        .id("XREF5")
                        .database(UniParcDatabase.SWISSPROT)
                        .build();
        Pair<String, List<UniParcCrossReference>> entry5 = new PairImpl<>(key5, List.of(xref5));
        store.saveOrUpdateEntry(entry5);

        // Ensure the entry is saved
        assertTrue(store.getEntry(key5).isPresent());

        // Truncate the store
        store.truncate();

        // Ensure the store is empty
        assertFalse(store.getEntry(key5).isPresent());
    }
}
