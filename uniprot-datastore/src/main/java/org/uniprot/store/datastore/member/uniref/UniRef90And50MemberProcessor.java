package org.uniprot.store.datastore.member.uniref;

import static org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore.getVoldemortKey;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore;

import lombok.extern.slf4j.Slf4j;

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
                                .map(VoldemortRemoteUniRefMemberStore::getVoldemortKey)
                                .collect(Collectors.toList()));

        Map<String, RepresentativeMember> existingMemberIdMember =
                existingMembers.stream()
                        .collect(
                                Collectors.toMap(
                                        VoldemortRemoteUniRefMemberStore::getVoldemortKey,
                                        eMember -> eMember));

        return members.stream()
                .map(
                        member ->
                                uniRefRepMemberPairMerger.apply(
                                        member,
                                        existingMemberIdMember.getOrDefault(
                                                getVoldemortKey(member), member)))
                .collect(Collectors.toList());
    }
}
