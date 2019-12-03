package org.uniprot.store.datastore.common;

import java.util.List;
import java.util.Optional;

/**
 * Base service for any data source/store
 *
 * @param <T>
 */
public interface StoreService<T> {
    List<T> getEntries(Iterable<String> ids);

    String getStoreName();

    Optional<T> getEntry(String id);
}
