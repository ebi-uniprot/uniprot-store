package org.uniprot.store.datastore.member.uniref.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.common.config.StoreProperties;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Configuration
@Profile("online")
public class UniRefMemberStoreConfig {

    @Bean
    @ConfigurationProperties(prefix = "store.uniref.member")
    public StoreProperties unirefMemberStoreProperties() {
        return new StoreProperties();
    }

    @Bean
    public UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient(
            StoreProperties storeProperties) {
        VoldemortClient<RepresentativeMember> client =
                new VoldemortRemoteUniRefMemberStore(
                        storeProperties.getNumberOfConnections(),
                        storeProperties.getStoreName(),
                        storeProperties.getHost());
        return new UniProtStoreClient<>(client);
    }
}
