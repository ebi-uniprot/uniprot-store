package org.uniprot.store.datastore.light.uniref;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortInMemoryUniRefEntryLightStore;

/**
 * @author jluo
 * @date: 20 Aug 2019
 */
@TestConfiguration
public class UniRefLightStoreTestConfig {
    @Bean
    @Profile("offline")
    public UniProtStoreClient<UniRefEntryLight> unirefLightStoreClient() {
        VoldemortClient<UniRefEntryLight> client =
                VoldemortInMemoryUniRefEntryLightStore.getInstance("uniref-light");
        return new UniProtStoreClient<>(client);
    }
}
