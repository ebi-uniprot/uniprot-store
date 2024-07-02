package org.uniprot.store.datastore.voldemort;

import java.util.Map;

import org.uniprot.store.datastore.common.StoreService;

/**
 * Represents a client of the Voldemort key / value data-store.
 *
 * @param <T> the entity type being stored in Voldemort.
 */
public interface VoldemortClient<T> extends StoreService<T>, AutoCloseable {
    Map<String, T> getEntryMap(Iterable<String> ids);

    void saveEntry(T entry);

    default void saveEntry(String key, T entry) {
        throw new UnsupportedOperationException("Override the method as per your need.");
    }

    void truncate();

    void close();

    void saveOrUpdateEntry(T entry);
}
