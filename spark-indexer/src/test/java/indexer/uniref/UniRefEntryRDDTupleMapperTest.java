package indexer.uniref;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.builder.UniProtAccessionBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.builder.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.builder.UniRefEntryBuilder;
import org.uniprot.core.uniref.builder.UniRefMemberBuilder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-22
 */
class UniRefEntryRDDTupleMapperTest {

    @Test
    void testMapRepresentativeUninprotKBMember() throws Exception{
        UniRefEntryRDDTupleMapper mapper = new UniRefEntryRDDTupleMapper();

        RepresentativeMember representativeMember = new RepresentativeMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPROTKB)
                .accession(new UniProtAccessionBuilder("P12345").build())
                .build();

        UniRefMember member = new UniRefMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPARC)
                .accession(new UniProtAccessionBuilder("UP1234567890").build())
                .build();

        UniRefEntry entry = new UniRefEntryBuilder()
                .entryType(UniRefType.UniRef100)
                .id("UniRef100_P12345")
                .memberCount(10)
                .representativeMember(representativeMember)
                .addMember(member)
                .build();

        Iterator<Tuple2<String, MappedUniRef>> result =  mapper.call(entry);
        assertNotNull(result);

        List<Tuple2<String, MappedUniRef>> resultList = new ArrayList<Tuple2<String, MappedUniRef>>();
        result.forEachRemaining(resultList::add);

        assertEquals(1, resultList.size());

        Tuple2<String, MappedUniRef> tuple = resultList.get(0);
        assertEquals("P12345", tuple._1);

        MappedUniRef mappedUniRef = tuple._2;
        assertNotNull(mappedUniRef);
        assertEquals("UniRef100_P12345", mappedUniRef.getClusterID());
        assertEquals(10, mappedUniRef.getMemberSize());
        assertEquals(representativeMember, mappedUniRef.getUniRefMember());
        assertEquals(UniRefType.UniRef100, mappedUniRef.getUniRefType());

        assertEquals(1, mappedUniRef.getMemberAccessions().size());
        assertTrue(mappedUniRef.getMemberAccessions().contains("P12345"));
    }


    @Test
    void testMapUniprotKBTypeMembers() throws Exception{
        UniRefEntryRDDTupleMapper mapper = new UniRefEntryRDDTupleMapper();

        RepresentativeMember representativeMember = new RepresentativeMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPARC)
                .accession(new UniProtAccessionBuilder("UP1234567890").build())
                .build();

        UniRefMember member = new UniRefMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPROTKB)
                .accession(new UniProtAccessionBuilder("P12345").build())
                .build();

        UniRefMember uniparcMember = new UniRefMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPARC)
                .accession(new UniProtAccessionBuilder("UP1234567899").build())
                .build();

        UniRefEntry entry = new UniRefEntryBuilder()
                .entryType(UniRefType.UniRef100)
                .id("UniRef100_P12345")
                .memberCount(10)
                .addMember(member)
                .addMember(uniparcMember)
                .representativeMember(representativeMember)
                .build();

        Iterator<Tuple2<String, MappedUniRef>> result =  mapper.call(entry);
        assertNotNull(result);

        List<Tuple2<String, MappedUniRef>> resultList = new ArrayList<Tuple2<String, MappedUniRef>>();
        result.forEachRemaining(resultList::add);

        assertEquals(1, resultList.size());

        Tuple2<String, MappedUniRef> tuple = resultList.get(0);
        assertEquals("P12345", tuple._1);

        MappedUniRef mappedUniRef = tuple._2;
        assertNotNull(mappedUniRef);
        assertEquals("UniRef100_P12345", mappedUniRef.getClusterID());
        assertEquals(10, mappedUniRef.getMemberSize());
        assertEquals(member, mappedUniRef.getUniRefMember());
        assertEquals(UniRefType.UniRef100, mappedUniRef.getUniRefType());

        assertEquals(1, mappedUniRef.getMemberAccessions().size());
        assertTrue(mappedUniRef.getMemberAccessions().contains("P12345"));
    }

}