package org.uniprot.store.datastore.member.uniref;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.impl.UniRefEntryIdBuilder;

/**
 * @@author sahmad
 * @created 28/07/2020
 */
public class UniRefRepMemberPairMergerTest {

    @Test
    void testMerge(){
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

        RepresentativeMember rm2 =
                new RepresentativeMemberBuilder()
                        .memberId(memberId)
                        .uniref50Id(new UniRefEntryIdBuilder("uniref50id").build())
                        .organismTaxId(0l)
                        .build();

        RepresentativeMember merged = new UniRefRepMemberPairMerger().apply(rm2, rm1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(type, merged.getMemberIdType());
        Assertions.assertEquals(memberId, merged.getMemberId());
        Assertions.assertEquals("Homo sapiens", merged.getOrganismName());
        Assertions.assertEquals(9606l, merged.getOrganismTaxId());
        Assertions.assertEquals(sequence, merged.getSequence());
        Assertions.assertEquals("uniref50id", merged.getUniRef50Id().getValue());
    }
}
