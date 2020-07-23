package org.uniprot.store.datastore.member.uniref;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.core.xml.uniref.RepresentativeMemberConverter;

/**
 * @author sahmad
 * @since 23/07/2020
 */
public class UniRefMemberProcessor implements ItemProcessor<MemberType, RepresentativeMember> {
    private final RepresentativeMemberConverter converter;

    public UniRefMemberProcessor() {
        converter = new RepresentativeMemberConverter();
    }

    @Override
    public RepresentativeMember process(MemberType memberType) throws Exception {
        return converter.fromXml(memberType);
    }
}
