package org.uniprot.store.datastore.uniparc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.common.config.StoreProperties;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
@Configuration
@Profile("online")
public class UniParcStoreConfig {

    @Bean
    @ConfigurationProperties(prefix = "store.uniparc")
    public StoreProperties uniParcStoreProperties() {
        return new StoreProperties();
    }

    @Bean
    public UniProtStoreClient<UniParcEntry> uniParcStoreClient(
            StoreProperties uniParcStoreProperties) {
        VoldemortClient<UniParcEntry> client =
                new VoldemortRemoteUniParcEntryStore(
                        uniParcStoreProperties.getNumberOfConnections(),
                        uniParcStoreProperties.getStoreName(),
                        uniParcStoreProperties.getHost());
        return new UniProtStoreClient<>(client);
    }
}
