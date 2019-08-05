package org.uniprot.store.datastore.uniprotkb.config;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortInMemoryUniprotEntryStore;

/**
 * Created 29/07/19
 *
 * @author Edd
 */
@TestConfiguration
public class StoreTestConfig {
    @Bean
    @Profile("offline")
    public UniProtStoreClient<UniProtEntry> uniProtKBStoreClient() {
        VoldemortClient<UniProtEntry> client = VoldemortInMemoryUniprotEntryStore.getInstance("avro-uniprot");
        return new UniProtStoreClient<>(client);
    }
}