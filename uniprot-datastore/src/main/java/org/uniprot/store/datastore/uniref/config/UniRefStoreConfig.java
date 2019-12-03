package org.uniprot.store.datastore.uniref.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.common.config.StoreProperties;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;

/**
 * @author jluo
 * @date: 15 Aug 2019
 */
@Configuration
@Profile("online")
public class UniRefStoreConfig {

    @Bean
    @ConfigurationProperties(prefix = "store.uniref")
    public StoreProperties unirefStoreProperties() {
        return new StoreProperties();
    }

    @Bean
    public UniProtStoreClient<UniRefEntry> unirefStoreClient(StoreProperties storeProperties) {
        VoldemortClient<UniRefEntry> client =
                new VoldemortRemoteUniRefEntryStore(
                        storeProperties.getNumberOfConnections(),
                        storeProperties.getStoreName(),
                        storeProperties.getHost());
        return new UniProtStoreClient<>(client);
    }
}
