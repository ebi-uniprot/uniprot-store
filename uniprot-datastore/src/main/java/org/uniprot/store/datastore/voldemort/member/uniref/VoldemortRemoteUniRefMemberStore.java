package org.uniprot.store.datastore.voldemort.member.uniref;

import org.uniprot.core.json.parser.uniref.UniRefEntryJsonConfig;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
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
        return getVoldemortKey(entry);
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniRefEntryJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<RepresentativeMember> getEntryClass() {
        return RepresentativeMember.class;
    }

    public static String getVoldemortKey(UniRefMember member) {
        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            return member.getMemberId();
        } else {
            return member.getUniProtAccessions().get(0).getValue();
        }
    }
}
