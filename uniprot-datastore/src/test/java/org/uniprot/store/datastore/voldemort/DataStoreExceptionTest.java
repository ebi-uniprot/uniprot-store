package org.uniprot.store.datastore.voldemort;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author lgonzales
 * @since 16/09/2020
 */
class DataStoreExceptionTest {

    @Test
    void testCreateException() {
        DataStoreException ex = new DataStoreException("message");
        assertNotNull(ex);
        assertEquals("message", ex.getMessage());
    }

    @Test
    void testCreateNullMessageException() {
        DataStoreException ex = new DataStoreException(null);
        assertNotNull(ex);
        assertNull(ex.getMessage());
    }

    @Test
    void testCreateExceptionWithCause() {
        DataStoreException ex = new DataStoreException("message", new Exception("internal"));
        assertNotNull(ex);
        assertEquals("message", ex.getMessage());
        assertNotNull(ex.getCause());
        assertEquals("internal", ex.getCause().getMessage());
    }
}
