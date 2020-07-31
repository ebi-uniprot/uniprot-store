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
 *
 * @created 28/07/2020
 */
class UniRefRepMemberPairMergerTest {

    @Test
    void testMerge() {
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
                        .organismTaxId(0L)
                        .build();

        RepresentativeMember merged = new UniRefRepMemberPairMerger().apply(rm2, rm1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(type, merged.getMemberIdType());
        Assertions.assertEquals(memberId, merged.getMemberId());
        Assertions.assertEquals("Homo sapiens", merged.getOrganismName());
        Assertions.assertEquals(9606L, merged.getOrganismTaxId());
        Assertions.assertEquals(sequence, merged.getSequence());
        Assertions.assertEquals("uniref50id", merged.getUniRef50Id().getValue());
    }

    @Test
    void testMergeUpdateMemberIdType() {
        String seq = "MVSWGRFICLVVVTMATLSLARPSFSLVED";
        Sequence sequence = new SequenceBuilder(seq).build();
        String memberId = "P12345";
        UniRefMemberIdType type = UniRefMemberIdType.UNIPROTKB;

        RepresentativeMember rm1 =
                new RepresentativeMemberBuilder()
                        .memberId(memberId)
                        .organismName("Homo sapiens")
                        .organismTaxId(9606)
                        .sequence(sequence)
                        .build();

        Assertions.assertNull(rm1.getUniRef50Id());

        RepresentativeMember rm2 =
                new RepresentativeMemberBuilder()
                        .memberIdType(type)
                        .uniref50Id(new UniRefEntryIdBuilder("uniref50id").build())
                        .organismTaxId(0L)
                        .build();

        RepresentativeMember merged = new UniRefRepMemberPairMerger().apply(rm2, rm1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(type, merged.getMemberIdType());
        Assertions.assertEquals(memberId, merged.getMemberId());
    }

    @Test
    void testMergeUpdateSequence() {
        String memberId = "P12345";
        UniRefMemberIdType type = UniRefMemberIdType.UNIPROTKB;

        RepresentativeMember rm1 =
                new RepresentativeMemberBuilder()
                        .memberIdType(type)
                        .memberId(memberId)
                        .organismName("Homo sapiens")
                        .organismTaxId(9606)
                        .build();

        Assertions.assertNull(rm1.getUniRef50Id());
        Assertions.assertNull(rm1.getSequence());

        String seq = "MVSWGRFICLVVVTMATLSLARPSFSLVED";
        Sequence sequence = new SequenceBuilder(seq).build();
        RepresentativeMember rm2 =
                new RepresentativeMemberBuilder()
                        .sequence(sequence)
                        .uniref50Id(new UniRefEntryIdBuilder("uniref50id").build())
                        .organismTaxId(0L)
                        .build();

        RepresentativeMember merged = new UniRefRepMemberPairMerger().apply(rm2, rm1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(type, merged.getMemberIdType());
        Assertions.assertEquals(memberId, merged.getMemberId());
        Assertions.assertEquals(sequence, merged.getSequence());
    }

    @Test
    void testMergeUpdateNullOrganismName() {
        String memberId = "P12345";
        UniRefMemberIdType type = UniRefMemberIdType.UNIPROTKB;

        RepresentativeMember rm1 =
                new RepresentativeMemberBuilder()
                        .memberIdType(type)
                        .memberId(memberId)
                        .organismTaxId(9606)
                        .build();

        Assertions.assertNull(rm1.getUniRef50Id());
        Assertions.assertNull(rm1.getOrganismName());
        RepresentativeMember rm2 =
                new RepresentativeMemberBuilder()
                        .uniref50Id(new UniRefEntryIdBuilder("uniref50id").build())
                        .organismTaxId(0L)
                        .build();

        RepresentativeMember merged = new UniRefRepMemberPairMerger().apply(rm2, rm1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(type, merged.getMemberIdType());
        Assertions.assertEquals(memberId, merged.getMemberId());
        Assertions.assertNull(merged.getOrganismName());
    }

    @Test
    void testMergeUpdateTaxId() {
        String memberId = "P12345";
        UniRefMemberIdType type = UniRefMemberIdType.UNIPROTKB;

        RepresentativeMember rm1 =
                new RepresentativeMemberBuilder().memberIdType(type).memberId(memberId).build();

        Assertions.assertNull(rm1.getUniRef50Id());
        Assertions.assertEquals(0L, rm1.getOrganismTaxId());
        RepresentativeMember rm2 =
                new RepresentativeMemberBuilder()
                        .uniref50Id(new UniRefEntryIdBuilder("uniref50id").build())
                        .organismTaxId(12345L)
                        .build();

        RepresentativeMember merged = new UniRefRepMemberPairMerger().apply(rm2, rm1);
        Assertions.assertNotNull(merged);
        Assertions.assertEquals(type, merged.getMemberIdType());
        Assertions.assertEquals(memberId, merged.getMemberId());
        Assertions.assertEquals(12345L, merged.getOrganismTaxId());
        Assertions.assertNull(merged.getOrganismName());
    }
}
