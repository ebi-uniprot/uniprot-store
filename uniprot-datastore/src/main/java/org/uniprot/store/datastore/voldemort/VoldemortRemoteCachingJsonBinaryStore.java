package org.uniprot.store.datastore.voldemort;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import org.springframework.cache.Cache;
import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

import java.util.*;

/**
 * This class extends a remote JSON Voldemort store with a caching layer. Caching criteria is based
 * on properties of the items being stored. Concrete implementations of this class must define the
 * {@link VoldemortRemoteCachingJsonBinaryStore#isCacheable(Object)} method, specifying the criteria
 * for caching.
 *
 * <p>Created 26/02/2020
 *
 * @author Edd
 */
@Slf4j
public abstract class VoldemortRemoteCachingJsonBinaryStore<T>
        extends VoldemortRemoteJsonBinaryStore<T> {
    private Cache cache;

    public VoldemortRemoteCachingJsonBinaryStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    public VoldemortRemoteCachingJsonBinaryStore(String storeName, String... voldemortUrl) {
        super(storeName, voldemortUrl);
    }

    public VoldemortRemoteCachingJsonBinaryStore(
            String storeName, StoreClient<String, byte[]> client) {
        super(storeName, client);
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> getEntries(Iterable<String> ids) {
        Map<String, T> entriesMap = new HashMap<>();

        // retrieve entries from cache if they exist
        List<String> idsToFetchFromStore = new ArrayList<>();
        for (String id : ids) {
            Cache.ValueWrapper valueWrapper = cache.get(id);
            if (valueWrapper != null) {
                entriesMap.put(id, (T) valueWrapper.get());
            } else {
                idsToFetchFromStore.add(id);
            }
        }

        // fetch entries that were not in cache
        try {
            Map<String, Versioned<byte[]>> notCachedBatch =
                    Failsafe.with(retryPolicy).get(() -> client.getAll(idsToFetchFromStore));
            idsToFetchFromStore.forEach(
                    id -> {
                        Versioned<byte[]> versionedEntry = notCachedBatch.get(id);
                        if (versionedEntry != null) {
                            T entry = getEntryFromBinary(versionedEntry);
                            entriesMap.put(id, entry);
                            cacheEntry(id, entry);
                        }
                    });
        } catch (Exception e) {
            log.warn("Error getting entry from Voldemort.", e);
            throw new RetrievalException("Error getting entry from Voldemort", e);
        }

        List<T> toReturn = new ArrayList<>();
        ids.forEach(id -> toReturn.add(entriesMap.get(id)));

        return toReturn;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<T> getEntry(String id) {
        try {
            Optional<T> toReturn;
            Cache.ValueWrapper valueWrapper = cache.get(id);
            if (valueWrapper != null) {
                toReturn = Optional.of((T) valueWrapper.get());
            } else {
                Versioned<byte[]> versionedEntry =
                        Failsafe.with(retryPolicy).get(() -> client.get(id));

                if (versionedEntry != null) {
                    T entry = getEntryFromBinary(versionedEntry);
                    cacheEntry(id, entry);
                    toReturn = Optional.ofNullable(entry);
                } else {
                    toReturn = Optional.empty();
                }
            }
            return toReturn;
        } catch (Exception e) {
            log.warn("Error getting entry from Voldemort.", e);
            throw new RetrievalException("Error getting entry from Voldemort", e);
        }
    }

    protected abstract boolean isCacheable(T item);

    private void cacheEntry(String id, T entry) {
        if (isCacheable(entry)) {
            cache.putIfAbsent(id, entry);
        }
    }
}
