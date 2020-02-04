package org.uniprot.store.spark.indexer.uniref;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-22
 */
class UniRefMapperTest {

    @Test
    void testUniRef50() throws Exception {
        UniRefMapper mapper = new UniRefMapper();

        MappedUniRef mappedUniRef =
                MappedUniRef.builder()
                        .uniRefType(UniRefType.UniRef50)
                        .uniparcUPI("UPI0009B2B4C6")
                        .clusterID("UniRef100_P21802")
                        .build();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple =
                new Tuple2<>(new UniProtDocument(), Optional.of(mappedUniRef));

        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertNotNull(result.unirefCluster50);
        assertNull(result.unirefCluster90);
        assertNull(result.unirefCluster100);

        assertEquals("UniRef100_P21802", result.unirefCluster50);
        assertEquals("UPI0009B2B4C6", result.uniparc);
    }

    @Test
    void testUniRef90() throws Exception {
        UniRefMapper mapper = new UniRefMapper();

        MappedUniRef mappedUniRef =
                MappedUniRef.builder()
                        .uniRefType(UniRefType.UniRef90)
                        .uniparcUPI("UPI0009B2B4C6")
                        .clusterID("UniRef100_P21802")
                        .build();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple =
                new Tuple2<>(new UniProtDocument(), Optional.of(mappedUniRef));

        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertNull(result.unirefCluster50);
        assertNotNull(result.unirefCluster90);
        assertNull(result.unirefCluster100);

        assertEquals("UniRef100_P21802", result.unirefCluster90);
        assertEquals("UPI0009B2B4C6", result.uniparc);
    }

    @Test
    void testUniRef100() throws Exception {
        UniRefMapper mapper = new UniRefMapper();

        MappedUniRef mappedUniRef =
                MappedUniRef.builder()
                        .uniRefType(UniRefType.UniRef100)
                        .uniparcUPI("UPI0009B2B4C6")
                        .clusterID("UniRef100_P21802")
                        .build();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple =
                new Tuple2<>(new UniProtDocument(), Optional.of(mappedUniRef));

        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertNull(result.unirefCluster50);
        assertNull(result.unirefCluster90);
        assertNotNull(result.unirefCluster100);

        assertEquals("UniRef100_P21802", result.unirefCluster100);
        assertEquals("UPI0009B2B4C6", result.uniparc);
    }

    @Test
    void testWithoutAnyMap() throws Exception {
        UniRefMapper mapper = new UniRefMapper();

        Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple =
                new Tuple2<>(new UniProtDocument(), Optional.empty());

        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertNull(result.unirefCluster50);
        assertNull(result.unirefCluster90);
        assertNull(result.unirefCluster100);
        assertNull(result.uniparc);
    }
}
