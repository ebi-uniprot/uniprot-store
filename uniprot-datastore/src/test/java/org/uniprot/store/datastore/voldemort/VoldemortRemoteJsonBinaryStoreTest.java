package org.uniprot.store.datastore.voldemort;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryBuilder;
import org.uniprot.core.cv.keyword.impl.KeywordIdBuilder;
import org.uniprot.core.json.parser.keyword.KeywordJsonConfig;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 01/10/2020
 */
class VoldemortRemoteJsonBinaryStoreTest {

    StoreClient<String, byte[]> client;
    public static final String STORE_NAME = "storeName";
    public static final String KEYWORD_ID = "KW-123";
    private VoldemortRemoteJsonBinaryStore<KeywordEntry> voldemort;
    private KeywordEntry entry;

    @BeforeEach
    void setupVoldemort() {
        client = Mockito.mock(StoreClient.class);
        voldemort = new FakeVoldemortRemoteJsonBinaryStore(STORE_NAME, "tcp://localhost:1010");
        entry =
                new KeywordEntryBuilder()
                        .keyword(new KeywordIdBuilder().id(KEYWORD_ID).build())
                        .build();
    }

    @Test
    void usingVoldemortRemoteUniParcEntryStoreConstructorThrowsException() {
        Assertions.assertThrows(
                RetrievalException.class,
                () -> new VoldemortRemoteUniParcEntryStore(10, "uniparc", "tcp://localhost:1010"));
    }

    @Test
    void close() {
        Assertions.assertDoesNotThrow(() -> voldemort.close());
    }

    @Test
    void canGetVoldemortProperties() {
        Properties result = voldemort.getVoldemortProperties(10);
        assertNotNull(result);
        assertEquals("10", result.getProperty(ClientConfig.MAX_CONNECTIONS_PER_NODE_PROPERTY));
        assertEquals(7, result.size());
    }

    @Test
    void saveEntry() {
        Assertions.assertDoesNotThrow(() -> voldemort.saveEntry(entry));
    }

    @Test
    void saveOrUpdateEntry() {
        Assertions.assertDoesNotThrow(() -> voldemort.saveOrUpdateEntry(entry));
    }

    @Test
    void getStoreId() {
        String id = voldemort.getStoreId(entry);
        assertNotNull(id);
        assertEquals(KEYWORD_ID, id);
    }

    @Test
    void getStoreObjectMapper() {
        ObjectMapper result = voldemort.getStoreObjectMapper();
        assertNotNull(result);
        assertTrue(result.canSerialize(KeywordEntry.class));
    }

    @Test
    void getEntryClass() {
        assertEquals(KeywordEntry.class, voldemort.getEntryClass());
    }

    @Test
    void getEntryValidAccession() throws Exception {
        Versioned<byte[]> versioned =
                new Versioned<>(voldemort.getStoreObjectMapper().writeValueAsBytes(entry));
        Mockito.when(client.get(Mockito.anyString())).thenReturn(versioned);
        Optional<KeywordEntry> result = voldemort.getEntry(KEYWORD_ID);
        assertTrue(result.isPresent());
        assertEquals(entry, result.get());
    }

    @Test
    void getEntryValidInvalidAccession() throws Exception {
        Versioned<byte[]> versioned =
                new Versioned<>(voldemort.getStoreObjectMapper().writeValueAsBytes(entry));
        Mockito.when(client.get(Mockito.same(KEYWORD_ID))).thenReturn(versioned);
        Optional<KeywordEntry> result = voldemort.getEntry("INVALID");
        assertFalse(result.isPresent());
    }

    @Test
    void getEntriesValidAccessions() throws Exception {
        Versioned<byte[]> versioned =
                new Versioned<>(voldemort.getStoreObjectMapper().writeValueAsBytes(entry));
        Map<String, Versioned<byte[]>> entryMap = new HashMap<>();
        entryMap.put(KEYWORD_ID, versioned);
        Mockito.when(client.getAll(Mockito.anyIterable())).thenReturn(entryMap);
        List<KeywordEntry> result = voldemort.getEntries(Collections.singletonList(KEYWORD_ID));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(entry, result.get(0));
    }

    @Test
    void getEntriesInvalidAccessions() throws Exception {
        Versioned<byte[]> versioned =
                new Versioned<>(voldemort.getStoreObjectMapper().writeValueAsBytes(entry));
        Map<String, Versioned<byte[]>> entryMap = new HashMap<>();
        entryMap.put(KEYWORD_ID, versioned);
        Mockito.when(client.getAll(Mockito.anyIterable())).thenReturn(entryMap);
        List<String> accessions = Arrays.asList(KEYWORD_ID, "INVALID");
        RetrievalException result =
                assertThrows(RetrievalException.class, () -> voldemort.getEntries(accessions));
        assertNotNull(result);
        assertEquals("Error getting entry from BDB store", result.getMessage());
    }

    @Test
    void getEntryMapValidAccession() throws Exception {
        Versioned<byte[]> versioned =
                new Versioned<>(voldemort.getStoreObjectMapper().writeValueAsBytes(entry));
        Map<String, Versioned<byte[]>> entryMap = new HashMap<>();
        entryMap.put(KEYWORD_ID, versioned);
        Mockito.when(client.getAll(Mockito.anyIterable())).thenReturn(entryMap);
        Map<String, KeywordEntry> result =
                voldemort.getEntryMap(Collections.singletonList(KEYWORD_ID));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(KEYWORD_ID));
        assertEquals(entry, result.get(KEYWORD_ID));
    }

    @Test
    void getEntryMapInvalidAccession() throws Exception {
        Versioned<byte[]> versioned =
                new Versioned<>(voldemort.getStoreObjectMapper().writeValueAsBytes(entry));
        Map<String, Versioned<byte[]>> entryMap = new HashMap<>();
        entryMap.put(KEYWORD_ID, versioned);
        Mockito.when(client.getAll(Mockito.anyIterable())).thenReturn(entryMap);
        List<String> accessions = Arrays.asList(KEYWORD_ID, "INVALID");
        Map<String, KeywordEntry> result = voldemort.getEntryMap(accessions);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(KEYWORD_ID));
    }

    @Test
    void truncateIsUnsupportedOperation() throws Exception {
        UnsupportedOperationException result =
                assertThrows(UnsupportedOperationException.class, () -> voldemort.truncate());
        assertNotNull(result);
        assertEquals(
                "Truncate remove voldemort is not a supported operation.", result.getMessage());
    }

    @Test
    void getStoreName() {
        assertEquals(STORE_NAME, voldemort.getStoreName());
    }

    private class FakeVoldemortRemoteJsonBinaryStore
            extends VoldemortRemoteJsonBinaryStore<KeywordEntry> {

        public FakeVoldemortRemoteJsonBinaryStore(String storeName, String... voldemortUrl) {
            super(storeName, voldemortUrl);
            ReflectionTestUtils.setField(this, "client", client);
        }

        @Override
        public String getStoreId(KeywordEntry entry) {
            return entry.getAccession();
        }

        @Override
        public ObjectMapper getStoreObjectMapper() {
            return KeywordJsonConfig.getInstance().getFullObjectMapper();
        }

        @Override
        public Class<KeywordEntry> getEntryClass() {
            return KeywordEntry.class;
        }

        @Override
        SocketStoreClientFactory getSocketClientFactory(ClientConfig clientConfig) {
            return new SocketStoreClientFactory(clientConfig);
        }

        @Override
        StoreClient<String, byte[]> getStoreClient(String storeName) {
            return client;
        }
    }
}
