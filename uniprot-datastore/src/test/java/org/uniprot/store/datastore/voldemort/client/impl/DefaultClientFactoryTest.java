package org.uniprot.store.datastore.voldemort.client.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.Mockito;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.client.UniProtClient;

class DefaultClientFactoryTest {

    @Test
    void testGetUniProtClient() throws IllegalAccessException {
        // When
        DefaultClientFactory factory = new DefaultClientFactory("url");
        VoldemortClient<UniProtKBEntry> voldemortClient = Mockito.mock(VoldemortClient.class);
        Field field =
                ReflectionUtils.findFields(
                                DefaultClientFactory.class,
                                f -> f.getName().equals("voldemortClient"),
                                ReflectionUtils.HierarchyTraversalMode.TOP_DOWN)
                        .get(0);

        field.setAccessible(true);
        field.set(factory, voldemortClient);
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
