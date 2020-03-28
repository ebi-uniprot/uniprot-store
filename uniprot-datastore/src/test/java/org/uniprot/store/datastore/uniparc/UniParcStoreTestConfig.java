package org.uniprot.store.datastore.uniparc;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortInMemoryUniParcEntryStore;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
@TestConfiguration
public class UniParcStoreTestConfig {

    @Bean
    @Profile("offline")
    public UniProtStoreClient<UniParcEntry> uniParcStoreClient() {
        VoldemortClient<UniParcEntry> client =
                VoldemortInMemoryUniParcEntryStore.getInstance("uniparc");
        return new UniProtStoreClient<>(client);
    }
}
