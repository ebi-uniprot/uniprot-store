package indexer.uniref;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.impl.UniParcIdImpl;
import org.uniprot.core.uniprot.builder.UniProtAccessionBuilder;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.core.uniref.builder.UniRefMemberBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import scala.Tuple2;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-22
 */
class UniRefMapperTest {

    @Test
    void testUniRef50() throws Exception{
        UniRefMapper mapper = new UniRefMapper();

        UniRefMember member = new UniRefMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPROTKB)
                .accession(new UniProtAccessionBuilder("P12345").build())
                .uniparcId(new UniParcIdImpl("UPI0009B2B4C6"))
                .build();

        MappedUniRef mappedUniRef = MappedUniRef.builder()
                .memberSize(12)
                .memberAccessions(Arrays.asList("P21802","P12345"))
                .uniRefType(UniRefType.UniRef50)
                .uniRefMember(member)
                .clusterID("UniRef100_P21802")
                .build();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple = new Tuple2<>(new UniProtDocument(), Optional.of(mappedUniRef));

        UniProtDocument result =  mapper.call(tuple);

        assertNotNull(result);

        assertEquals(3, result.unirefCluster50.size());
        assertTrue(result.unirefCluster90.isEmpty());
        assertTrue(result.unirefCluster100.isEmpty());

        assertTrue(result.unirefCluster50.contains("UniRef100_P21802"));
        assertTrue(result.unirefCluster50.contains("P21802"));
        assertTrue(result.unirefCluster50.contains("P12345"));

        assertEquals(12, result.unirefSize50);
        assertEquals(0, result.unirefSize90);
        assertEquals(0, result.unirefSize100);

        assertEquals("UPI0009B2B4C6", result.uniparc);
    }

    @Test
    void testUniRef90() throws Exception{
        UniRefMapper mapper = new UniRefMapper();

        UniRefMember member = new UniRefMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPROTKB)
                .accession(new UniProtAccessionBuilder("P12345").build())
                .uniparcId(new UniParcIdImpl("UPI0009B2B4C6"))
                .build();

        MappedUniRef mappedUniRef = MappedUniRef.builder()
                .memberSize(12)
                .memberAccessions(Arrays.asList("P21802","P12345"))
                .uniRefType(UniRefType.UniRef90)
                .uniRefMember(member)
                .clusterID("UniRef100_P21802")
                .build();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple = new Tuple2<>(new UniProtDocument(), Optional.of(mappedUniRef));

        UniProtDocument result =  mapper.call(tuple);

        assertNotNull(result);

        assertTrue(result.unirefCluster50.isEmpty());
        assertEquals(3, result.unirefCluster90.size());
        assertTrue(result.unirefCluster100.isEmpty());

        assertTrue(result.unirefCluster90.contains("UniRef100_P21802"));
        assertTrue(result.unirefCluster90.contains("P21802"));
        assertTrue(result.unirefCluster90.contains("P12345"));

        assertEquals(0, result.unirefSize50);
        assertEquals(12, result.unirefSize90);
        assertEquals(0, result.unirefSize100);

        assertEquals("UPI0009B2B4C6", result.uniparc);
    }


    @Test
    void testUniRef100() throws Exception{
        UniRefMapper mapper = new UniRefMapper();

        UniRefMember member = new UniRefMemberBuilder()
                .memberIdType(UniRefMemberIdType.UNIPROTKB)
                .accession(new UniProtAccessionBuilder("P12345").build())
                .uniparcId(new UniParcIdImpl("UPI0009B2B4C6"))
                .build();

        MappedUniRef mappedUniRef = MappedUniRef.builder()
                .memberSize(12)
                .memberAccessions(Arrays.asList("P21802","P12345"))
                .uniRefType(UniRefType.UniRef100)
                .uniRefMember(member)
                .clusterID("UniRef100_P21802")
                .build();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple = new Tuple2<>(new UniProtDocument(), Optional.of(mappedUniRef));

        UniProtDocument result =  mapper.call(tuple);

        assertNotNull(result);

        assertTrue(result.unirefCluster50.isEmpty());
        assertTrue(result.unirefCluster90.isEmpty());
        assertEquals(3, result.unirefCluster100.size());

        assertTrue(result.unirefCluster100.contains("UniRef100_P21802"));
        assertTrue(result.unirefCluster100.contains("P21802"));
        assertTrue(result.unirefCluster100.contains("P12345"));

        assertEquals(0, result.unirefSize50);
        assertEquals(0, result.unirefSize90);
        assertEquals(12, result.unirefSize100);

        assertEquals("UPI0009B2B4C6", result.uniparc);
    }


    @Test
    void testWithoutAnyMap() throws Exception{
        UniRefMapper mapper = new UniRefMapper();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple = new Tuple2<>(new UniProtDocument(), Optional.empty());

        UniProtDocument result =  mapper.call(tuple);

        assertNotNull(result);

        assertTrue(result.unirefCluster50.isEmpty());
        assertTrue(result.unirefCluster90.isEmpty());
        assertTrue(result.unirefCluster100.isEmpty());

        assertEquals(0, result.unirefSize50);
        assertEquals(0, result.unirefSize90);
        assertEquals(0, result.unirefSize100);

        assertNull(result.uniparc);
    }
}