package org.uniprot.store.datastore.voldemort.light.uniparc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;

class VoldemortInMemoryUniParcEntryLightStoreTest {

    private VoldemortInMemoryUniParcEntryLightStore store;

    @BeforeEach
    void setUp() {
        store = VoldemortInMemoryUniParcEntryLightStore.getInstance("uniparc-light");
    }

    @AfterEach
    void tearDown() {
        store.truncate();
    }

    @Test
    void testSaveAndGetEntry() {
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();

        // Create UniParcEntryLight using the builder
        UniParcEntryLight entry =
                builder.uniParcId("UP000000001")
                        .crossReferenceCount(2)
                        .uniProtKBAccessionsAdd("P12345")
                        .build();

        // Save the entry
        store.saveOrUpdateEntry(entry);

        // Retrieve the entry
        UniParcEntryLight retrievedEntry = store.getEntry("UP000000001").orElse(null);

        // Assert that the retrieved entry matches the saved entry
        assertNotNull(retrievedEntry);
        assertEquals("UP000000001", retrievedEntry.getUniParcId());
        assertEquals(2, retrievedEntry.getCrossReferenceCount());
        assertEquals(Set.of("P12345"), retrievedEntry.getUniProtKBAccessions());
    }

    @Test
    void testSaveAndUpdateEntry() {
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();

        // Create and save an entry
        UniParcEntryLight entry =
                builder.uniParcId("UP000000002")
                        .crossReferenceCount(3)
                        .uniProtKBAccessionsAdd("P67890")
                        .build();
        store.saveOrUpdateEntry(entry);

        // Update the entry using builder
        entry =
                builder.uniParcId("UP000000002")
                        .crossReferenceCount(4)
                        .uniProtKBAccessionsAdd("Q98765")
                        .build();
        store.saveOrUpdateEntry(entry);

        // Retrieve the updated entry
        UniParcEntryLight retrievedEntry = store.getEntry("UP000000002").orElse(null);

        // Assert that the retrieved entry reflects the update
        assertNotNull(retrievedEntry);
        assertEquals("UP000000002", retrievedEntry.getUniParcId());
        assertEquals(4, retrievedEntry.getCrossReferenceCount());
        assertEquals(Set.of("P67890", "Q98765"), retrievedEntry.getUniProtKBAccessions());
    }

    @Test
    void testGetEntryMap() {
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();

        // Create and save multiple entries
        UniParcEntryLight entry1 =
                builder.uniParcId("UP000000003")
                        .crossReferenceCount(7)
                        .uniProtKBAccessionsAdd("P11111")
                        .build();
        UniParcEntryLight entry2 =
                builder.uniParcId("UP000000004")
                        .crossReferenceCount(9)
                        .uniProtKBAccessionsAdd("P22222")
                        .build();
        store.saveOrUpdateEntry(entry1);
        store.saveOrUpdateEntry(entry2);

        // Retrieve entries as a map
        Map<String, UniParcEntryLight> entryMap =
                store.getEntryMap(Arrays.asList("UP000000003", "UP000000004"));

        // Assert that both entries are retrieved correctly
        assertEquals(2, entryMap.size());
        assertTrue(entryMap.containsKey("UP000000003"));
        assertTrue(entryMap.containsKey("UP000000004"));
    }
}
