package org.uniprot.store.spark.indexer.uniref.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.Iterator;

import org.junit.jupiter.api.Test;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.impl.UniRefEntryBuilder;
import org.uniprot.core.uniref.impl.UniRefMemberBuilder;

import scala.Tuple2;

/**
 * @@author sahmad
 *
 * @created 21/07/2020
 */
class UniRefToMembersTest {
    @Test
    void testUniRefWithOnlyRepMemberToMembers() throws Exception {

        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .accessionsAdd(new UniProtKBAccessionBuilder("P12345").build())
                        .organismTaxId(1)
                        .organismName("name")
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        UniRefEntry entry =
                new UniRefEntryBuilder()
                        .representativeMember(representativeMember)
                        .entryType(UniRefType.UniRef50)
                        .id("id")
                        .name("name")
                        .updated(LocalDate.now())
                        .build();

        UniRefToMembers mapper = new UniRefToMembers();
        Iterator<Tuple2<String, RepresentativeMember>> tuples = mapper.call(entry);
        int tupleCount = 0;
        assertNotNull(tuples);
        while (tuples.hasNext()) {
            Tuple2<String, RepresentativeMember> tuple = tuples.next();
            assertEquals("P12345", tuple._1);
            assertEquals(representativeMember, tuple._2);
            tupleCount++;
        }
        assertEquals(1, tupleCount, "Tuple count not equal to expected");
    }

    @Test
    void testUniRefToMembers() throws Exception {

        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPARC)
                        .memberId("UPI001298EA76")
                        .accessionsAdd(new UniProtKBAccessionBuilder("P12345").build())
                        .organismTaxId(1)
                        .organismName("name")
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        UniRefMember member1 =
                new UniRefMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .memberId("P54321_dummy")
                        .accessionsAdd(new UniProtKBAccessionBuilder("P54321").build())
                        .build();

        UniRefMember member2 =
                new UniRefMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPARC)
                        .memberId("UPI001298EA77")
                        .accessionsAdd(new UniProtKBAccessionBuilder("Q12345").build())
                        .build();

        UniRefEntry entry =
                new UniRefEntryBuilder()
                        .representativeMember(representativeMember)
                        .membersAdd(member1)
                        .membersAdd(member2)
                        .entryType(UniRefType.UniRef50)
                        .id("id")
                        .name("name")
                        .updated(LocalDate.now())
                        .build();

        UniRefToMembers mapper = new UniRefToMembers();
        Iterator<Tuple2<String, RepresentativeMember>> tuples = mapper.call(entry);
        int tupleCount = 0;
        assertNotNull(tuples);
        while (tuples.hasNext()) {
            Tuple2<String, RepresentativeMember> tuple = tuples.next();
            assertTrue(
                    "UPI001298EA76".equals(tuple._1)
                            || "P54321".equals(tuple._1)
                            || "UPI001298EA77".equals(tuple._1));
            assertTrue(
                    representativeMember.getMemberId().equals(tuple._2.getMemberId())
                            || member1.getMemberId().equals(tuple._2.getMemberId())
                            || member2.getMemberId().equals(tuple._2.getMemberId()));
            tupleCount++;
        }
        assertEquals(3, tupleCount, "Tuple count not equal to expected");
    }
}
