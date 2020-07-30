package org.uniprot.store.datastore.member.uniref;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.core.xml.uniref.MemberConverter;
import org.uniprot.core.xml.uniref.RepresentativeMemberConverter;

/**
 * @@author sahmad
 *
 * @created 28/07/2020
 */
public abstract class BaseUniRefMemberProcessor<I, O> implements ItemProcessor<I, O> {
    private final RepresentativeMemberConverter repMemberConverter;
    private final MemberConverter memberConverter;

    public BaseUniRefMemberProcessor() {
        repMemberConverter = new RepresentativeMemberConverter();
        memberConverter = new MemberConverter();
    }

    protected RepresentativeMember convert(MemberType memberType) {
        RepresentativeMemberBuilder builder;

        if (Objects.nonNull(memberType.getSequence())) {
            RepresentativeMember repMember = repMemberConverter.fromXml(memberType);
            builder = RepresentativeMemberBuilder.from(repMember).memberId(getMemberId(repMember));
        } else {
            UniRefMember member = memberConverter.fromXml(memberType);
            builder = RepresentativeMemberBuilder.from(member).memberId(getMemberId(member));
        }
        // update seed to null
        return builder.isSeed(null).build();
    }

    protected List<RepresentativeMember> convert(List<MemberType> memberTypes) {
        return memberTypes.stream().map(this::convert).collect(Collectors.toList());
    }

    // get accession id if memberType is UniRefMemberIdType.UNIPROTKB
    private String getMemberId(UniRefMember member) {
        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            return member.getMemberId();
        } else {
            return member.getUniProtAccessions().get(0).getValue();
        }
    }
}
