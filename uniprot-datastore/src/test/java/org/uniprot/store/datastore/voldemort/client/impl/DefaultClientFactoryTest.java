package org.uniprot.store.datastore.voldemort.client.impl;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

class DefaultClientFactoryTest {

    @Test
    void testGetUniProtClient() {
        // When
        DefaultClientFactory factory = Mockito.mock(DefaultClientFactory.class);
        // then
        assertDoesNotThrow(factory::createUniProtClient);
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
