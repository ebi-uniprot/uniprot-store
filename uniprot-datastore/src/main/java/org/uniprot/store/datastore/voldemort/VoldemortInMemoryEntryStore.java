package org.uniprot.store.datastore.voldemort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.MockStoreClientFactory;
import voldemort.serialization.StringSerializer;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.versioning.Versioned;

import java.util.*;

/**
 * Created 18/04/2016
 *
 * <p>Use an in Memory storage to build a EntryStore directly. Mostly used in test cases scenarios.
 *
 * @author wudong
 */
public abstract class VoldemortInMemoryEntryStore<T> implements VoldemortClient<T> {

    private static final Logger logger = LoggerFactory.getLogger(VoldemortInMemoryEntryStore.class);
    private final String storeName;

    private StorageEngine<String, T, String> storageEngine;

    public VoldemortInMemoryEntryStore(String storeName) {
        this.storeName = storeName;
        MockStoreClientFactory mockStoreClientFactory = new MockStoreClientFactory(new StringSerializer(), new StringSerializer(), new StringSerializer());
        mockStoreClientFactory.getStoreClient(this.storeName);
        this.storageEngine = new InMemoryStorageEngine<>(this.storeName);
    }

    public Store<String, T, String> getStore() {
        return this.storageEngine;
    }

    @Override
    public void saveEntry(T entry) {
        String id = getStoreId(entry);
        logger.info("Saving entry: {}", id);

        Store<String, T, String> store = this.getStore();
        Versioned<T> versionedEntry = new Versioned<>(entry);
        store.put(id, versionedEntry, this.storeName);
    }

    public void updateEntry(T entry) {
        String id = getStoreId(entry);
        logger.info("Updating entry: {}", id);
        Store<String, T, String> store = this.getStore();
        List<Versioned<T>> listVersionedEntry = store.get(id, this.storeName);
        if (listVersionedEntry != null && !listVersionedEntry.isEmpty()) {
            Versioned<T> currentEntry = listVersionedEntry.get(0);
            currentEntry.setObject(entry);
            store.delete(id, currentEntry.getVersion());
            store.put(id, currentEntry, this.storeName);
        }
    }

    public void truncate() {
        logger.info("Truncating entries ");
        storageEngine.truncate();
    }

    public Optional<T> getEntry(String id) {
        logger.info("Getting entry: {}", id);
        Store<String, T, String> store = this.getStore();

        Optional<T> result = Optional.empty();
        List<Versioned<T>> listVersionedEntry = store.get(id, this.storeName);
        if (listVersionedEntry != null && !listVersionedEntry.isEmpty()) {
            result = Optional.of(listVersionedEntry.get(0).getValue());
        }
        return result;
    }

    public List<T> getEntries(Iterable<String> acc) {
        logger.info("Getting entry list : {}", acc);
        List<T> result = new ArrayList<>();
        acc.forEach(
                accession -> {
                    Optional<T> entry = getEntry(accession);
                    entry.ifPresent(result::add);
                });
        return result;
    }

    public Map<String, T> getEntryMap(Iterable<String> acc) {
        Store<String, T, String> store = this.getStore();
        Map<String, List<Versioned<T>>> all = store.getAll(acc, null);
        HashMap<String, T> stringEntryObjectHashMap = new HashMap<>();

        all.forEach((key, value) -> stringEntryObjectHashMap.put(key, value.get(0).getValue()));

        return stringEntryObjectHashMap;
    }

    public String getStoreName() {
        return this.storeName;
    }

    public abstract String getStoreId(T entry);
}
