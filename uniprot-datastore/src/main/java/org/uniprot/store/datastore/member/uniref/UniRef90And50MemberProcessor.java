package org.uniprot.store.datastore.member.uniref;

import java.util.Objects;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.core.xml.uniref.MemberConverter;
import org.uniprot.core.xml.uniref.RepresentativeMemberConverter;
import org.uniprot.store.datastore.UniProtStoreClient;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Slf4j
public class UniRef90And50MemberProcessor
        implements ItemProcessor<MemberType, RepresentativeMember> {
    private final RepresentativeMemberConverter repMemberConverter;
    private final MemberConverter memberConverter;
    private final UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient;

    public UniRef90And50MemberProcessor(
            UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient) {
        repMemberConverter = new RepresentativeMemberConverter();
        memberConverter = new MemberConverter();
        this.unirefMemberStoreClient = unirefMemberStoreClient;
    }

    @Override
    public RepresentativeMember process(MemberType memberType) throws Exception {
        RepresentativeMemberBuilder builder;

        if (Objects.nonNull(memberType.getSequence())) {
            RepresentativeMember repMember = repMemberConverter.fromXml(memberType);
            builder = RepresentativeMemberBuilder.from(repMember).memberId(getMemberId(repMember));
        } else {
            UniRefMember member = memberConverter.fromXml(memberType);
            builder = RepresentativeMemberBuilder.from(member).memberId(getMemberId(member));
        }
        // update seed to null
        RepresentativeMember repMember = builder.isSeed(null).build();

        // get from voldemort store and enrich it with repMember(uniref90/uniref50 member)
        Optional<RepresentativeMember> existingMember =
                this.unirefMemberStoreClient.getEntry(repMember.getMemberId());
        if (existingMember.isPresent()) {
            log.info("Member {} exist in Voldemort store", repMember.getMemberId());
            return mergeSourceToTarget(repMember, existingMember.get());
        }

        log.warn("Member {} doesn't exist in Voldemort store", repMember.getMemberId());
        return repMember;
    }

    // get accession id if memberType is UniRefMemberIdType.UNIPROTKB
    private String getMemberId(UniRefMember member) {
        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            return member.getMemberId();
        } else {
            return member.getUniProtAccessions().get(0).getValue();
        }
    }

    // set value from source to target if target doesnt have already set
    private RepresentativeMember mergeSourceToTarget(
            RepresentativeMember source, RepresentativeMember target) {
        RepresentativeMemberBuilder builder = RepresentativeMemberBuilder.from(target);

        if (Objects.isNull(target.getMemberIdType()) && Objects.nonNull(source.getMemberIdType())) {
            builder.memberIdType(source.getMemberIdType());
        }

        if (Utils.nullOrEmpty(target.getOrganismName())
                && Utils.notNullNotEmpty(source.getOrganismName())) {
            builder.organismName(source.getOrganismName());
        }

        if (target.getOrganismTaxId() == 0l && source.getOrganismTaxId() != 0l) {
            builder.organismTaxId(source.getOrganismTaxId());
        }

        if (target.getSequenceLength() == 0 && source.getSequenceLength() != 0) {
            builder.sequenceLength(source.getSequenceLength());
        }

        if (Utils.nullOrEmpty(target.getProteinName())
                && Utils.notNullNotEmpty(source.getProteinName())) {
            builder.proteinName(source.getProteinName());
        }

        if (Utils.nullOrEmpty(target.getUniProtAccessions())
                && Utils.notNullNotEmpty(source.getUniProtAccessions())) {
            builder.accessionsSet(source.getUniProtAccessions());
        }
        if (Objects.isNull(target.getUniRef50Id()) && Objects.nonNull(source.getUniRef50Id())) {
            builder.uniref50Id(source.getUniRef50Id());
        }
        if (Objects.isNull(target.getUniRef90Id()) && Objects.nonNull(source.getUniRef90Id())) {
            builder.uniref90Id(source.getUniRef90Id());
        }

        if (Objects.isNull(target.getUniRef100Id()) && Objects.nonNull(source.getUniRef100Id())) {
            builder.uniref100Id(source.getUniRef100Id());
        }

        if (Objects.isNull(target.getUniParcId()) && Objects.nonNull(source.getUniParcId())) {
            builder.uniparcId(source.getUniParcId());
        }

        if (Objects.isNull(target.getOverlapRegion())
                && Objects.nonNull(source.getOverlapRegion())) {
            builder.overlapRegion(source.getOverlapRegion());
        }

        return builder.build();
    }
}
