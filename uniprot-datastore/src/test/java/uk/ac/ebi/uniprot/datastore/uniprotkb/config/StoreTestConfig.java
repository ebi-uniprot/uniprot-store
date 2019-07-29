package uk.ac.ebi.uniprot.datastore.uniprotkb.config;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.uniprot.datastore.UniProtStoreClient;
import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortClient;
import uk.ac.ebi.uniprot.datastore.voldemort.uniprot.VoldemortInMemoryUniprotEntryStore;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;

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