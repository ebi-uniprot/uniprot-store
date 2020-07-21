package org.uniprot.store.spark.indexer.uniref.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;

import scala.Tuple2;

/**
 * @@author sahmad
 *
 * @created 21/07/2020
 */
class UniRefMemberMergerTest {
    @Test
    void testUniRefRepMemberAndMemberMerge() throws Exception {
        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .accessionsAdd(new UniProtKBAccessionBuilder("P12345").build())
                        .organismTaxId(1)
                        .organismName("name")
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        RepresentativeMember memberWithoutSeq =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .accessionsAdd(new UniProtKBAccessionBuilder("Q12345").build())
                        .organismTaxId(2)
                        .sequenceLength(12)
                        .organismName("name1")
                        .build();
        Tuple2<RepresentativeMember, RepresentativeMember> tuple =
                new Tuple2<>(representativeMember, memberWithoutSeq);

        UniRefMemberMerger mapper = new UniRefMemberMerger();
        RepresentativeMember mergedMember = mapper.call(tuple);
        assertNotNull(mergedMember);
        assertEquals(0, representativeMember.getSequenceLength());
        assertEquals(memberWithoutSeq.getSequenceLength(), mergedMember.getSequenceLength());
    }

    @Test
    void testUniRefMemberAndMemberMerge() throws Exception {
        RepresentativeMember memberWithoutSeq1 =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .accessionsAdd(new UniProtKBAccessionBuilder("P12345").build())
                        .organismTaxId(1)
                        .organismName("name")
                        .proteinName("new protein name")
                        .build();

        RepresentativeMember memberWithoutSeq2 =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .accessionsAdd(new UniProtKBAccessionBuilder("Q12345").build())
                        .organismTaxId(2)
                        .organismName("name1")
                        .build();
        Tuple2<RepresentativeMember, RepresentativeMember> tuple =
                new Tuple2<>(memberWithoutSeq1, memberWithoutSeq2);

        UniRefMemberMerger mapper = new UniRefMemberMerger();
        RepresentativeMember mergedMember = mapper.call(tuple);
        assertNotNull(mergedMember);
        assertNull(memberWithoutSeq2.getProteinName());
        assertEquals(memberWithoutSeq1.getProteinName(), mergedMember.getProteinName());
    }
}
