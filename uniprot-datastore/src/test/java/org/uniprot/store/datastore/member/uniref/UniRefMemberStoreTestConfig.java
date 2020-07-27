package org.uniprot.store.datastore.member.uniref;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortInMemoryUniRefMemberStore;

/**
 * @author sahmad
 * @date: 27 July 2020
 */
@TestConfiguration
public class UniRefMemberStoreTestConfig {
    @Bean
    @Profile("offline")
    public UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient() {
        VoldemortClient<RepresentativeMember> client =
                VoldemortInMemoryUniRefMemberStore.getInstance("uniref-member");
        return new UniProtStoreClient<>(client);
    }
}
