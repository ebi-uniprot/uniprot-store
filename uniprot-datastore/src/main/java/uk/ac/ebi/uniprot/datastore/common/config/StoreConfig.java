package uk.ac.ebi.uniprot.datastore.common.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.uniprot.datastore.UniProtStoreClient;
import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortClient;
import uk.ac.ebi.uniprot.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;

/**
 * Created 27/07/19
 *
 * @author Edd
 */
@Configuration
@EnableConfigurationProperties({StoreProperties.class})
public class StoreConfig {
    private final StoreProperties storeProperties;

    @Autowired
    public StoreConfig(StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }

    @Bean
    @Profile("online")
    public UniProtStoreClient<UniProtEntry> uniProtStoreClient() {
        VoldemortClient<UniProtEntry> client =
                new VoldemortRemoteUniProtKBEntryStore(storeProperties.getNumberOfConnections(),
                                                       storeProperties.getStoreName(),
                                                       storeProperties.getHost());
        return new UniProtStoreClient<>(client);
    }
}
