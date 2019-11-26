package org.uniprot.store.datastore.uniref;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortInMemoryUniRefEntryStore;

/**
 * @author jluo
 * @date: 20 Aug 2019
 */
@TestConfiguration
public class UniRefStoreTestConfig {
    @Bean
    @Profile("offline")
    public UniProtStoreClient<UniRefEntry> unirefStoreClient() {
        VoldemortClient<UniRefEntry> client =
                VoldemortInMemoryUniRefEntryStore.getInstance("uniref");
        return new UniProtStoreClient<>(client);
    }
}
