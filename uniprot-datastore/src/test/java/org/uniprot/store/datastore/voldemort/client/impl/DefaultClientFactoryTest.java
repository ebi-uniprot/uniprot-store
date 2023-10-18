package org.uniprot.store.datastore.voldemort.client.impl;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.client.UniProtClient;

class DefaultClientFactoryTest {

    @Test
    void testGetUniProtClient() {
        // When
        DefaultClientFactory factory = new DefaultClientFactory("url");
        VoldemortClient<UniProtKBEntry> voldemortClient = Mockito.mock(VoldemortClient.class);
        ReflectionTestUtils.setField(factory, "voldemortClient", voldemortClient);
        // then
        UniProtClient uniProtClient = factory.createUniProtClient();
        assertNotNull(uniProtClient);
    }

    @Test
    void createUniProtClientWithUrlOnly() {
        // Given
        String url = "sampleUrl";

        // When
        DefaultClientFactory factory = new DefaultClientFactory(url);

        // Then
        RuntimeException exception =
                assertThrows(RuntimeException.class, factory::createUniProtClient);
        assertEquals("Voldemort Store initialization failed.", exception.getMessage());
        assertDoesNotThrow(() -> factory.close());
    }

    @Test
    void createUniProtClientWithUrlAndConnectionOnly() {
        // Given
        String url = "sampleUrl";
        int numberOfConn = 10;

        // When
        DefaultClientFactory factory = new DefaultClientFactory(url, numberOfConn);

        // Then
        RuntimeException exception =
                assertThrows(RuntimeException.class, factory::createUniProtClient);
        assertEquals("Voldemort Store initialization failed.", exception.getMessage());
    }

    @Test
    void createUniProtClientWithBrotliEnabled() {
        // Given
        String voldemortUrl = "sampleUrl";
        int numberOfConn = 10;
        String storeName = "avro-uniprot";
        boolean brotliEnabled = true;
        int compressionLevel = 10;

        // When
        DefaultClientFactory factory =
                new DefaultClientFactory(
                        voldemortUrl, numberOfConn, storeName, brotliEnabled, compressionLevel);

        // Then
        RuntimeException exception =
                assertThrows(RuntimeException.class, factory::createUniProtClient);
        assertEquals("Voldemort Store initialization failed.", exception.getMessage());
    }

    @Test
    void createUniProtClientWithInvalidStore() {
        // Given
        String invalidUrl = "invalidUrl";
        int numberOfConn = 5;
        String storeName = "invalidStore";

        // When
        DefaultClientFactory factory =
                new DefaultClientFactory(invalidUrl, numberOfConn, storeName);

        // Then
        RuntimeException exception =
                assertThrows(RuntimeException.class, factory::createUniProtClient);
        assertEquals("Voldemort Store initialization failed.", exception.getMessage());
    }
}
