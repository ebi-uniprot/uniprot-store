package org.uniprot.store.datastore.member.uniref;

import static org.uniprot.store.datastore.voldemort.member.uniref.VoldemortInMemoryUniRefMemberStore.getMemberId;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortInMemoryUniRefMemberStore;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Slf4j
public class UniRef90And50MemberProcessor
        extends BaseUniRefMemberProcessor<List<MemberType>, List<RepresentativeMember>> {
    private final UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient;
    private final UniRefRepMemberPairMerger uniRefRepMemberPairMerger;

    public UniRef90And50MemberProcessor(
            UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient) {
        super();
        this.unirefMemberStoreClient = unirefMemberStoreClient;
        uniRefRepMemberPairMerger = new UniRefRepMemberPairMerger();
    }

    @Override
    public List<RepresentativeMember> process(List<MemberType> memberTypes) throws Exception {
        List<RepresentativeMember> members = convert(memberTypes);

        List<RepresentativeMember> existingMembers =
                this.unirefMemberStoreClient.getEntries(
                        members.stream()
                                .map(VoldemortInMemoryUniRefMemberStore::getMemberId)
                                .collect(Collectors.toList()));

        Map<String, RepresentativeMember> existingMemberIdMember =
                existingMembers.stream()
                        .collect(
                                Collectors.toMap(
                                        VoldemortInMemoryUniRefMemberStore::getMemberId,
                                        eMember -> eMember));

        return members.stream()
                .map(
                        member ->
                                uniRefRepMemberPairMerger.apply(
                                        member,
                                        existingMemberIdMember.getOrDefault(
                                                getMemberId(member), member)))
                .collect(Collectors.toList());
    }
}
