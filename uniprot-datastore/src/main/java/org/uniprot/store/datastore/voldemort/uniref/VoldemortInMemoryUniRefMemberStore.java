package org.uniprot.store.datastore.voldemort.uniref;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

/**
 * @author lgonzales
 * @since 22/07/2020
 */
public class VoldemortInMemoryUniRefMemberStore extends VoldemortInMemoryEntryStore<RepresentativeMember> {

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
    public String getStoreId(RepresentativeMember member) {
        if(member.getMemberIdType().equals(UniRefMemberIdType.UNIPARC)){
            return member.getMemberId();
        } else {
            return member.getUniProtAccessions().get(0).getValue();
        }
    }
}

