package org.uniprot.store.datastore.member.uniref;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;

/**
 * @author sahmad
 * @since 23/07/2020
 */
public class UniRef100MemberProcessor extends BaseUniRefMemberProcessor {
    public UniRef100MemberProcessor() {
        super();
    }

    @Override
    public RepresentativeMember process(MemberType memberType) throws Exception {
        return convert(memberType);
    }
}
