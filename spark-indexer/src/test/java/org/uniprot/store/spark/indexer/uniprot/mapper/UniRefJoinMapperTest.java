package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.impl.UniParcIdBuilder;
import org.uniprot.core.uniprot.impl.UniProtAccessionBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.impl.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.impl.UniRefEntryBuilder;
import org.uniprot.core.uniref.impl.UniRefMemberBuilder;
import org.uniprot.store.spark.indexer.uniprot.mapper.model.MappedUniRef;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-22
 */
class UniRefJoinMapperTest {

    @Test
    void testMapRepresentativeUninprotKBMember() throws Exception {
        UniRefJoinMapper mapper = new UniRefJoinMapper();

        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .accessionsAdd(new UniProtAccessionBuilder("P12345").build())
                        .uniparcId(new UniParcIdBuilder("UPI000000111").build())
                        .build();

        UniRefMember member =
                new UniRefMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPARC)
                        .accessionsAdd(new UniProtAccessionBuilder("UP1234567890").build())
                        .build();

        UniRefEntry entry =
                new UniRefEntryBuilder()
                        .entryType(UniRefType.UniRef100)
                        .id("UniRef100_P12345")
                        .memberCount(10)
                        .representativeMember(representativeMember)
                        .membersAdd(member)
                        .build();

        Iterator<Tuple2<String, MappedUniRef>> result = mapper.call(entry);
        assertNotNull(result);

        List<Tuple2<String, MappedUniRef>> resultList =
                new ArrayList<Tuple2<String, MappedUniRef>>();
        result.forEachRemaining(resultList::add);

        assertEquals(1, resultList.size());

        Tuple2<String, MappedUniRef> tuple = resultList.get(0);
        assertEquals("P12345", tuple._1);

        MappedUniRef mappedUniRef = tuple._2;
        assertNotNull(mappedUniRef);
        assertEquals("UniRef100_P12345", mappedUniRef.getClusterID());
        assertEquals("UPI000000111", mappedUniRef.getUniparcUPI());
        assertEquals(UniRefType.UniRef100, mappedUniRef.getUniRefType());
    }

    @Test
    void testMapUniprotKBTypeMembers() throws Exception {
        UniRefJoinMapper mapper = new UniRefJoinMapper();

        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPARC)
                        .accessionsAdd(new UniProtAccessionBuilder("UP1234567890").build())
                        .build();

        UniRefMember member =
                new UniRefMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPROTKB)
                        .accessionsAdd(new UniProtAccessionBuilder("P12345").build())
                        .uniparcId(new UniParcIdBuilder("UPI000000111").build())
                        .build();

        UniRefMember uniparcMember =
                new UniRefMemberBuilder()
                        .memberIdType(UniRefMemberIdType.UNIPARC)
                        .accessionsAdd(new UniProtAccessionBuilder("UP1234567899").build())
                        .build();

        UniRefEntry entry =
                new UniRefEntryBuilder()
                        .entryType(UniRefType.UniRef100)
                        .id("UniRef100_P12345")
                        .memberCount(10)
                        .membersAdd(member)
                        .membersAdd(uniparcMember)
                        .representativeMember(representativeMember)
                        .build();

        Iterator<Tuple2<String, MappedUniRef>> result = mapper.call(entry);
        assertNotNull(result);

        List<Tuple2<String, MappedUniRef>> resultList =
                new ArrayList<Tuple2<String, MappedUniRef>>();
        result.forEachRemaining(resultList::add);

        assertEquals(1, resultList.size());

        Tuple2<String, MappedUniRef> tuple = resultList.get(0);
        assertEquals("P12345", tuple._1);

        MappedUniRef mappedUniRef = tuple._2;
        assertNotNull(mappedUniRef);
        assertEquals("UniRef100_P12345", mappedUniRef.getClusterID());
        assertEquals("UPI000000111", mappedUniRef.getUniparcUPI());
        assertEquals(UniRefType.UniRef100, mappedUniRef.getUniRefType());
    }
}
