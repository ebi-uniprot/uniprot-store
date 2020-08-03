package org.uniprot.store.datastore.voldemort.member.uniref;

import static org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore.getVoldemortKey;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

/**
 * @author sahmad
 * @since 21/07/2020
 */
public class VoldemortInMemoryUniRefMemberStore
        extends VoldemortInMemoryEntryStore<RepresentativeMember> {

    private static VoldemortInMemoryUniRefMemberStore instance;

    public static VoldemortInMemoryUniRefMemberStore getInstance(String storeName) {
        if (instance == null) {
            instance = new VoldemortInMemoryUniRefMemberStore(storeName);
        }
        return instance;
    }

    private VoldemortInMemoryUniRefMemberStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(RepresentativeMember entry) {
        return getVoldemortKey(entry);
    }
}
