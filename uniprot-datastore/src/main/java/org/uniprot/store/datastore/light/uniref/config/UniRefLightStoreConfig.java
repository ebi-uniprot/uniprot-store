package org.uniprot.store.datastore.light.uniref.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.common.config.StoreProperties;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortRemoteUniRefEntryLightStore;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
@Configuration
@Profile("online")
public class UniRefLightStoreConfig {

    @Bean
    @ConfigurationProperties(prefix = "store.uniref.light")
    public StoreProperties unirefLightStoreProperties() {
        return new StoreProperties();
    }

    @Bean
    public UniProtStoreClient<UniRefEntryLight> unirefLightStoreClient(
            StoreProperties storeProperties) {
        VoldemortClient<UniRefEntryLight> client =
                new VoldemortRemoteUniRefEntryLightStore(
                        storeProperties.getNumberOfConnections(),
                        storeProperties.getStoreName(),
                        storeProperties.getHost());
        return new UniProtStoreClient<>(client);
    }
}
