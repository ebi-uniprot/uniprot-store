package org.uniprot.store.datastore.voldemort.uniref;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.uniprot.core.json.parser.uniref.UniRefEntryJsonConfig;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

/**
 * @author lgonzales
 * @since 22/07/2020
 */
public class VoldemortRemoteUniRefMemberStore  extends VoldemortRemoteJsonBinaryStore<RepresentativeMember> {

    public VoldemortRemoteUniRefMemberStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(RepresentativeMember member) {
        if(member.getMemberIdType().equals(UniRefMemberIdType.UNIPARC)){
            return member.getMemberId();
        } else {
            return member.getUniProtAccessions().get(0).getValue();
        }
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

