package org.uniprot.store.datastore.voldemort;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore.DEFAULT_BROTLI_COMPRESSION_LEVEL;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.Mockito;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryBuilder;
import org.uniprot.core.cv.keyword.impl.KeywordIdBuilder;
import org.uniprot.core.json.parser.keyword.KeywordJsonConfig;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortRemoteUniRefEntryLightStore;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;

import com.aayushatharva.brotli4j.encoder.Encoder;
import com.fasterxml.jackson.databind.ObjectMapper;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * @author lgonzales
 * @since 01/10/2020
 */
class VoldemortRemoteJsonBinaryStoreTest {

    StoreClient<String, byte[]> client;
    public static final String STORE_NAME = "storeName";
    public static final String KEYWORD_ID = "KW-123";
    private FakeVoldemortRemoteJsonBinaryStore voldemort;
    private KeywordEntry entry;

    @BeforeEach
    void setupVoldemort() throws Exception {
        client = Mockito.mock(StoreClient.class);
        Mockito.when(client.put(Mockito.eq("KW-111"), Mockito.any(byte[].class)))
                .thenThrow(new ObsoleteVersionException("Error"));
        voldemort = new FakeVoldemortRemoteJsonBinaryStore(STORE_NAME, "tcp://localhost:99999999");
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
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniParcEntryStore(
                                10, false, "uniparc", "tcp://localhost:1010"));
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniParcEntryStore(
                                10,
                                true,
                                DEFAULT_BROTLI_COMPRESSION_LEVEL,
                                "uniparc",
                                "tcp://localhost:1010"));
    }

    @Test
    void getSocketClientFactoryMockingValidValuesDoesNotThrowException() {
        FailureDetector mockedDetector = Mockito.mock(FailureDetector.class);
        Mockito.when(mockedDetector.getAvailableNodeCount()).thenReturn(2);
        SocketStoreClientFactory mockedFactory = Mockito.mock(SocketStoreClientFactory.class);
        Mockito.when(mockedFactory.getFailureDetector()).thenReturn(mockedDetector);

        ClientConfig clientConfig = new ClientConfig(voldemort.getVoldemortProperties(2));
        clientConfig.setSocketBufferSize(1024 * 1204);
        clientConfig.setBootstrapUrls("tcp://localhost:1010");

        FakeVoldemortRemoteJsonBinaryStore mockedVoldemort =
                Mockito.mock(FakeVoldemortRemoteJsonBinaryStore.class);
        Mockito.when(mockedVoldemort.callSuperGetSocketClientFactory(Mockito.any()))
                .thenCallRealMethod();
        Mockito.when(mockedVoldemort.getClientFactory(Mockito.any())).thenReturn(mockedFactory);
        Assertions.assertDoesNotThrow(
                () -> mockedVoldemort.callSuperGetSocketClientFactory(clientConfig));
    }

    @Test
    void getSocketClientFactoryThrows() {
        FailureDetector mockedDetector = Mockito.mock(FailureDetector.class);
        Mockito.when(mockedDetector.getAvailableNodeCount()).thenReturn(0);
        SocketStoreClientFactory mockedFactory = Mockito.mock(SocketStoreClientFactory.class);
        Mockito.when(mockedFactory.getFailureDetector()).thenReturn(mockedDetector);

        ClientConfig clientConfig = new ClientConfig(voldemort.getVoldemortProperties(2));
        clientConfig.setSocketBufferSize(1024 * 1204);
        clientConfig.setBootstrapUrls("tcp://localhost:1010");

        FakeVoldemortRemoteJsonBinaryStore mockedVoldemort =
                Mockito.mock(FakeVoldemortRemoteJsonBinaryStore.class);
        Mockito.when(mockedVoldemort.callSuperGetSocketClientFactory(Mockito.any()))
                .thenCallRealMethod();
        Mockito.when(mockedVoldemort.getClientFactory(Mockito.any())).thenReturn(mockedFactory);
        assertThrows(
                RetrievalException.class,
                () -> mockedVoldemort.callSuperGetSocketClientFactory(clientConfig));
    }

    @Test
    void close() {
        Assertions.assertDoesNotThrow(() -> voldemort.close());
    }

    @Test
    void getSuperStoreClientWillThrowsError() {
        StoreClient<String, byte[]> result = voldemort.callSuperStoreClient(STORE_NAME);
        assertNotNull(result);
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
    void saveObsoleteIgnored() {
        KeywordEntry obsolete =
                new KeywordEntryBuilder()
                        .keyword(new KeywordIdBuilder().id("KW-111").build())
                        .build();
        Assertions.assertDoesNotThrow(() -> voldemort.saveEntry(obsolete));
    }

    @Test
    void saveOrUpdateEntry() {
        Assertions.assertDoesNotThrow(() -> voldemort.saveOrUpdateEntry(entry));
    }

    @Test
    void saveEntryWithKeyThrowsException() {
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> voldemort.saveEntry("samplekey", entry));
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
        Versioned<byte[]> versioned = getKeywordEntry();
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
    void getEntryReturnRetrievalException() throws Exception {
        Mockito.when(client.get(Mockito.eq("ERROR_ID")))
                .thenThrow(new RuntimeException("Mocked Exception"));
        assertThrows(RetrievalException.class, () -> voldemort.getEntry("ERROR_ID"));
    }

    @Test
    void getEntriesValidAccessions() throws Exception {
        Versioned<byte[]> versioned = getKeywordEntry();
        Map<String, Versioned<byte[]>> entryMap = new HashMap<>();
        entryMap.put(KEYWORD_ID, versioned);
        Mockito.when(client.getAll(Mockito.anyIterable())).thenReturn(entryMap);
        List<KeywordEntry> result = voldemort.getEntries(Collections.singletonList(KEYWORD_ID));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(entry, result.get(0));
    }

    @Test
    void getEntriesDoNotReturnInvalidAccessions() throws Exception {
        Versioned<byte[]> versioned = getKeywordEntry();
        Map<String, Versioned<byte[]>> entryMap = new HashMap<>();
        entryMap.put(KEYWORD_ID, versioned);
        Mockito.when(client.getAll(Mockito.anyIterable())).thenReturn(entryMap);
        List<String> accessions = Arrays.asList(KEYWORD_ID, "INVALID");
        List<KeywordEntry> result = voldemort.getEntries(accessions);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(entry, result.get(0));
    }

    @Test
    void getEntryMapValidAccession() throws Exception {
        Versioned<byte[]> versioned = getKeywordEntry();
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
        Versioned<byte[]> versioned = getKeywordEntry();
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

    @Test
    void saveAndGetNonBrotliCompressed() throws Exception {
        FakeVoldemortRemoteJsonBinaryStore nonBrotliVoldemort =
                new FakeVoldemortRemoteJsonBinaryStore(
                        false, STORE_NAME, "tcp://localhost:99999999");
        Assertions.assertDoesNotThrow(() -> nonBrotliVoldemort.saveEntry(entry));
        byte[] binaryEntry = nonBrotliVoldemort.getStoreObjectMapper().writeValueAsBytes(entry);
        Mockito.when(client.get(Mockito.anyString())).thenReturn(new Versioned<>(binaryEntry));
        Optional<KeywordEntry> result = nonBrotliVoldemort.getEntry(KEYWORD_ID);
        assertTrue(result.isPresent());
        assertEquals(entry, result.get());
    }

    @Test
    void usingVoldemortRemoteUniProtKBEntryStoreConstructorThrowsException() {
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniProtKBEntryStore(
                                10, "uniprot", "tcp://localhost:1010"));
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniProtKBEntryStore(
                                10, false, "uniprot", "tcp://localhost:1010"));
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniProtKBEntryStore(
                                10,
                                true,
                                DEFAULT_BROTLI_COMPRESSION_LEVEL,
                                "uniparc",
                                "tcp://localhost:1010"));
    }

    @Test
    void usingVoldemortRemoteUniRefLightEntryStoreConstructorThrowsException() {
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniRefEntryLightStore(
                                10, "uniref", "tcp://localhost:1010"));
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniRefEntryLightStore(
                                10, false, "uniref", "tcp://localhost:1010"));
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniRefEntryLightStore(
                                10,
                                true,
                                DEFAULT_BROTLI_COMPRESSION_LEVEL,
                                "uniref",
                                "tcp://localhost:1010"));
    }

    @Test
    void usingVoldemortRemoteUniRefMemberStoreConstructorThrowsException() {
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniRefMemberStore(
                                10, "uniref-member", "tcp://localhost:1010"));
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniRefMemberStore(
                                10, false, "uniref-member", "tcp://localhost:1010"));
        Assertions.assertThrows(
                RetrievalException.class,
                () ->
                        new VoldemortRemoteUniRefMemberStore(
                                10,
                                true,
                                DEFAULT_BROTLI_COMPRESSION_LEVEL,
                                "uniref-member",
                                "tcp://localhost:1010"));
    }

    private Versioned<byte[]> getKeywordEntry() throws IOException {
        byte[] binaryEntry = voldemort.getStoreObjectMapper().writeValueAsBytes(entry);
        byte[] compressedEntry =
                Encoder.compress(
                        binaryEntry,
                        new Encoder.Parameters().setQuality(DEFAULT_BROTLI_COMPRESSION_LEVEL));
        return new Versioned<>(compressedEntry);
    }

    private class FakeVoldemortRemoteJsonBinaryStore
            extends VoldemortRemoteJsonBinaryStore<KeywordEntry> {

        public FakeVoldemortRemoteJsonBinaryStore(String storeName, String... voldemortUrl)
                throws IllegalAccessException {
            this(BROTLI_ENABLED, storeName, voldemortUrl);
        }

        public FakeVoldemortRemoteJsonBinaryStore(
                boolean brotliEnabled, String storeName, String... voldemortUrl)
                throws IllegalAccessException {
            super(DEFAULT_MAX_CONNECTION, brotliEnabled, storeName, voldemortUrl);
            Field field =
                    ReflectionUtils.findFields(
                                    FakeVoldemortRemoteJsonBinaryStore.class,
                                    f -> f.getName().equals("client"),
                                    ReflectionUtils.HierarchyTraversalMode.TOP_DOWN)
                            .get(0);
            field.setAccessible(true);
            field.set(this, client);
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

        SocketStoreClientFactory callSuperGetSocketClientFactory(ClientConfig clientConfig) {
            return super.getSocketClientFactory(clientConfig);
        }

        @Override
        StoreClient<String, byte[]> getStoreClient(String storeName) {
            return client;
        }

        StoreClient<String, byte[]> callSuperStoreClient(String storeName) {
            return super.getStoreClient(storeName);
        }
    }
}
