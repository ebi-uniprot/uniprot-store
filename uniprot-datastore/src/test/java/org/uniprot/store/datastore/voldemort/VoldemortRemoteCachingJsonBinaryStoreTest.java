package org.uniprot.store.datastore.voldemort;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.Cache;
import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

/**
 * Created 26/02/2020
 *
 * @author Edd
 */
@ExtendWith(MockitoExtension.class)
class VoldemortRemoteCachingJsonBinaryStoreTest {
    @Mock private StoreClient<String, byte[]> client;
    @Mock private Cache cache;
    private FakeCachingJsonBinaryStore cachingStore;

    @BeforeEach
    void setUp() {
        cachingStore = createCachingStore();
    }

    @Test
    void canCreateClient() {
        assertThat(cachingStore, is(notNullValue()));
    }

    @Nested
    @DisplayName("Tests for getEntries")
    class GetEntriesTest {
        @Test
        void whenFetchingNonCachedEntry_thenFetchFromClient_andCacheEntry() {
            // given
            String id = "cacheable-1";
            Entry entry = new Entry(id, "1's data");
            List<String> ids = singletonList(id);
            when(cache.get(id)).thenReturn(null);
            when(client.getAll(ids)).thenReturn(versionedMapOf(entry));

            // when
            List<Entry> entries = cachingStore.getEntries(ids);

            // then
            verify(client).getAll(ids);
            assertThat(entries, contains(entry));
            verify(cache).putIfAbsent(id, entry);
        }

        @Test
        void whenFetchingNonCachedEntry_thenFetchFromClient_andDoNotCacheEntry() {
            // given
            String id = "not-cacheable-1";
            Entry entry = new Entry(id, "1's data");
            List<String> ids = singletonList(id);
            when(cache.get(id)).thenReturn(null);
            when(client.getAll(ids)).thenReturn(versionedMapOf(entry));

            // when
            List<Entry> entries = cachingStore.getEntries(ids);

            // then
            verify(client).getAll(ids);
            assertThat(entries, contains(entry));
            verify(cache, times(0)).putIfAbsent(id, entry);
        }

        @Test
        void whenFetchingCachedEntry_thenDoNotFetchFromClient_andDoNotCacheEntry() {
            // given
            String id = "cacheable-1";
            Entry entry = new Entry(id, "1's data");
            List<String> ids = singletonList(id);
            when(cache.get(id)).thenReturn(() -> entry);

            // when
            List<Entry> entries = cachingStore.getEntries(ids);

            // then
            verify(client, times(0)).getAll(ids);
            assertThat(entries, contains(entry));
            verify(cache, times(0)).putIfAbsent(id, entry);
        }

        @Test
        void whenFetchingCachedAndNotCachedEntry_ensureBothAreReceived() {
            // given
            String id1 = "cacheable-1";
            Entry entry1 = new Entry(id1, "1's data");
            String id2 = "not-cacheable-2";
            Entry entry2 = new Entry(id2, "2's data");

            List<String> ids = asList(id1, id2);
            when(cache.get(anyString()))
                    .thenReturn(() -> entry1) // for id1
                    .thenReturn(null); // for id2
            when(client.getAll(singletonList(id2))).thenReturn(versionedMapOf(entry2));

            // when
            List<Entry> entries = cachingStore.getEntries(ids);

            // then
            verify(cache, times(0)).putIfAbsent(anyString(), any()); // nothing cached
            verify(client, times(1)).getAll(singletonList(id2)); // only id2 retrieved from store
            assertThat(entries, contains(entry1, entry2)); // correct result set
        }

        @Test
        void whenErrorFromStore_thenThrowException() {
            doThrow(IllegalStateException.class).when(client).getAll(singletonList("id-1"));

            assertThrows(
                    RetrievalException.class, () -> cachingStore.getEntries(singletonList("id-1")));
        }
    }

    @Nested
    @DisplayName("Tests for getEntry")
    class GetEntryTest {
        @Test
        void whenFetchingNonCachedEntry_thenFetchFromClient_andCacheEntry() {
            // given
            String id = "cacheable-1";
            Entry entry = new Entry(id, "1's data");
            when(cache.get(id)).thenReturn(null);
            when(client.get(id)).thenReturn(versioned(entry));

            // when
            Optional<Entry> optionalRetrievedEntry = cachingStore.getEntry(id);

            // then
            verify(client).get(id);
            Entry retrievedEntry =
                    optionalRetrievedEntry.orElseThrow(() -> new AssertionError("Failed"));
            assertThat(retrievedEntry, is(entry));
            verify(cache).putIfAbsent(id, entry);
        }

        @Test
        void whenFetchingNonCachedEntry_thenFetchFromClient_andDoNotCacheEntry() {
            // given
            String id = "not-cacheable-1";
            Entry entry = new Entry(id, "1's data");
            when(cache.get(id)).thenReturn(null);
            when(client.get(id)).thenReturn(versioned(entry));

            // when
            Optional<Entry> optionalRetrievedEntry = cachingStore.getEntry(id);

            // then
            verify(client).get(id);
            Entry retrievedEntry =
                    optionalRetrievedEntry.orElseThrow(() -> new AssertionError("Failed"));
            assertThat(retrievedEntry, is(entry));
            verify(cache, times(0)).putIfAbsent(id, entry);
        }

        @Test
        void whenFetchingCachedEntry_thenDoNotFetchFromClient_andDoNotCacheEntry() {
            // given
            String id = "cacheable-1";
            Entry entry = new Entry(id, "1's data");
            when(cache.get(id)).thenReturn(() -> entry);

            // when
            Optional<Entry> optionalRetrievedEntry = cachingStore.getEntry(id);

            // then
            verify(client, times(0)).get(id);
            Entry retrievedEntry =
                    optionalRetrievedEntry.orElseThrow(() -> new AssertionError("Failed"));
            assertThat(retrievedEntry, is(entry));
            verify(cache, times(0)).putIfAbsent(id, entry);
        }

        @Test
        void whenClientReturnsNull_thenReturnEmpty() {
            // given
            String id = "cacheable-1";
            when(cache.get(id)).thenReturn(null);
            when(client.get(id)).thenReturn(null);

            // when
            Optional<Entry> optionalRetrievedEntry = cachingStore.getEntry(id);

            // then
            assertThat(optionalRetrievedEntry.isPresent(), is(false));
        }

        @Test
        void whenErrorFromStore_thenThrowException() {
            doThrow(IllegalStateException.class).when(client).get("id-1");

            assertThrows(RetrievalException.class, () -> cachingStore.getEntry("id-1"));
        }
    }

    private Map<String, Versioned<byte[]>> versionedMapOf(Entry... entries) {
        Map<String, Versioned<byte[]>> map = new HashMap<>();

        for (Entry entry : entries) {
            try {
                map.put(
                        entry.getId(),
                        new Versioned<>(
                                cachingStore.getStoreObjectMapper().writeValueAsBytes(entry)));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }

        return map;
    }

    private Versioned<byte[]> versioned(Entry entry) {
        try {
            return new Versioned<>(cachingStore.getStoreObjectMapper().writeValueAsBytes(entry));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private FakeCachingJsonBinaryStore createCachingStore() {
        FakeCachingJsonBinaryStore store = new FakeCachingJsonBinaryStore("fakeStore", client);
        store.setCache(cache);
        return store;
    }

    @Data
    private static class Entry {
        private String id;
        private String value;

        Entry() {}

        Entry(String id, String value) {
            this.id = id;
            this.value = value;
        }
    }

    private static class FakeCachingJsonBinaryStore
            extends VoldemortRemoteCachingJsonBinaryStore<Entry> {
        private final ObjectMapper objectMapper;

        public FakeCachingJsonBinaryStore(String storeName, StoreClient<String, byte[]> client) {
            super(storeName, client);
            this.objectMapper = new ObjectMapper();
        }

        @Override
        public String getStoreId(Entry entry) {
            return null;
        }

        @Override
        public ObjectMapper getStoreObjectMapper() {
            return objectMapper;
        }

        @Override
        public Class<Entry> getEntryClass() {
            return Entry.class;
        }

        @Override
        protected boolean isCacheable(Entry item) {
            return item.getId().startsWith("cacheable");
        }
    }
}
