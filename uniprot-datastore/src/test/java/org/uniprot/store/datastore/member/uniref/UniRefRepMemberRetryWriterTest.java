package org.uniprot.store.datastore.member.uniref;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;

/**
 * @@author sahmad
 * @created 28/07/2020
 */
public class UniRefRepMemberRetryWriterTest {

    @Test
    void testEntryToString(){
        String seq = "MVSWGRFICLVVVTMATLSLARPSFSLVED";
        Sequence sequence = new SequenceBuilder(seq).build();
        String memberId = "P12345";
        UniRefMemberIdType type = UniRefMemberIdType.UNIPROTKB;

        RepresentativeMember rm1 =
                new RepresentativeMemberBuilder()
                        .memberIdType(type)
                        .memberId(memberId)
                        .organismName("Homo sapiens")
                        .organismTaxId(9606)
                        .sequence(sequence)
                        .build();
        Assertions.assertNull(rm1.getUniRef50Id());

        UniRefMemberRetryWriter writer = new UniRefMemberRetryWriter(null, null);
        String xmlObjStr = writer.entryToString(rm1);
        Assertions.assertNotNull(xmlObjStr);
        Assertions.assertEquals(memberId, xmlObjStr);
    }
}
