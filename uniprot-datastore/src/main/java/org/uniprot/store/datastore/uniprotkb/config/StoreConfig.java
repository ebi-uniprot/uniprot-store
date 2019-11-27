package org.uniprot.store.datastore.uniprotkb.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.common.config.StoreProperties;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;

/**
 * Created 27/07/19
 *
 * @author Edd
 */
@Configuration
@Profile("online")
public class StoreConfig {
    @Bean
    @ConfigurationProperties(prefix = "store.uniprotkb")
    public StoreProperties uniprotKBStoreProperties() {
        return new StoreProperties();
    }

    @Bean
    public UniProtStoreClient<UniProtEntry> uniProtKBStoreClient(
            StoreProperties uniprotKBStoreProperties) {
        VoldemortClient<UniProtEntry> client =
                new VoldemortRemoteUniProtKBEntryStore(
                        uniprotKBStoreProperties.getNumberOfConnections(),
                        uniprotKBStoreProperties.getStoreName(),
                        uniprotKBStoreProperties.getHost());
        return new UniProtStoreClient<>(client);
    }
}
