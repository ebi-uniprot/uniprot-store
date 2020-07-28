package org.uniprot.store.datastore.member.uniref;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.store.datastore.UniProtStoreClient;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Slf4j
public class UniRef90And50MemberProcessor extends BaseUniRefMemberProcessor {
    private final UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient;
    private final UniRefRepMemberPairMerger uniRefRepMemberPairMerger;

    public UniRef90And50MemberProcessor(
            UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient) {
        super();
        this.unirefMemberStoreClient = unirefMemberStoreClient;
        uniRefRepMemberPairMerger = new UniRefRepMemberPairMerger();
    }

    @Override
    public RepresentativeMember process(MemberType memberType) throws Exception {
        RepresentativeMember repMember = convert(memberType);

        // get from voldemort store and enrich it with repMember(uniref90/uniref50 member)
        Optional<RepresentativeMember> existingMember =
                this.unirefMemberStoreClient.getEntry(repMember.getMemberId());
        if (existingMember.isPresent()) {
            log.debug("Member {} exist in Voldemort store", repMember.getMemberId());
            return this.uniRefRepMemberPairMerger.apply(repMember, existingMember.get());
        }

        log.debug("Member {} doesn't exist in Voldemort store", repMember.getMemberId());
        return repMember;
    }
}
