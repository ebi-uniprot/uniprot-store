package org.uniprot.store.datastore.voldemort.member.uniref;

import static org.uniprot.store.datastore.voldemort.member.uniref.VoldemortInMemoryUniRefMemberStore.getMemberId;

import org.uniprot.core.json.parser.uniref.UniRefEntryJsonConfig;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @since 21/07/2020
 */
public class VoldemortRemoteUniRefMemberStore
        extends VoldemortRemoteJsonBinaryStore<RepresentativeMember> {

    public VoldemortRemoteUniRefMemberStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(RepresentativeMember entry) {
        return getMemberId(entry);
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniRefEntryJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<RepresentativeMember> getEntryClass() {
        return RepresentativeMember.class;
    }
}
