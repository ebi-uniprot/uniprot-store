package org.uniprot.store.datastore.voldemort;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.cache.Cache;
import voldemort.client.StoreClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

/**
 * Created 26/02/2020
 *
 * @author Edd
 */
class VoldemortRemoteCachingJsonBinaryStoreTest {
    @Mock
    private StoreClient<String, byte[]> client;
    @Mock
    private Cache cache;
    private String storeName = "fakeStore";

    @Test
    void canCreateClient() {
        assertThat(createCachingStore(), is(notNullValue()));
    }

    @Test
    void whenFetchingNonCachedEntry_thenFetchFromClient_andCacheEntry() {
        Entry entry = Entry.builder().id("cacheable-1").value("1's data").build();
        when(cache.get("cacheable-1")).thenReturn(() -> entry);
    }

    @Test
    void whenFetchingNonCachedEntry_thenFetchFromClient_andDoNotCacheEntry() {

    }

    @Test
    void whenFetching() {

    }

    private FakeCachingJsonBinaryStore createCachingStore() {
        FakeCachingJsonBinaryStore store = new FakeCachingJsonBinaryStore(storeName, client);
        store.setCache(cache);
        return store;
    }

    @Builder
    @Getter
    private static class Entry {
        private String id;
        private String value;
    }

    private static class FakeCachingJsonBinaryStore extends VoldemortRemoteCachingJsonBinaryStore<Entry> {
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
