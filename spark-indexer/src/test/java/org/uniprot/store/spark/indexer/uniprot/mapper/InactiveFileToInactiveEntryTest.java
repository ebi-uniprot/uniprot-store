package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.EntryInactiveReason;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtkbEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class InactiveFileToInactiveEntryTest {

    @Test
    void testDeletedEntry() throws Exception {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        Tuple2<String, UniProtkbEntry> result = mapper.call("I8FBX0,I8FBX0_MYCAB,deleted,-");
        assertNotNull(result);

        assertNotNull(result._1);
        assertEquals("I8FBX0", result._1);

        assertNotNull(result._2);
        UniProtkbEntry entry = result._2;

        assertNotNull(entry);
        assertNotNull(entry.getPrimaryAccession());
        assertEquals("I8FBX0", entry.getPrimaryAccession().getValue());
        assertNotNull(entry.getUniProtkbId());
        assertEquals("I8FBX0_MYCAB", entry.getUniProtkbId().getValue());

        EntryInactiveReason inactiveReason = entry.getInactiveReason();
        assertNotNull(inactiveReason);

        assertNotNull(inactiveReason.getInactiveReasonType());
        assertEquals(InactiveReasonType.DELETED, inactiveReason.getInactiveReasonType());

        assertNotNull(inactiveReason.getMergeDemergeTos());
        assertTrue(inactiveReason.getMergeDemergeTos().isEmpty());
    }

    @Test
    void testMergedEntry() throws Exception {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        Tuple2<String, UniProtkbEntry> result = mapper.call("Q00220,SOMA_HYPMO  ,merged ,P69159");
        assertNotNull(result);

        assertNotNull(result._1);
        assertEquals("Q00220", result._1);

        assertNotNull(result._2);
        UniProtkbEntry entry = result._2;

        assertNotNull(entry);
        assertNotNull(entry.getPrimaryAccession());
        assertEquals("Q00220", entry.getPrimaryAccession().getValue());
        assertNotNull(entry.getUniProtkbId());
        assertEquals("SOMA_HYPMO", entry.getUniProtkbId().getValue());

        EntryInactiveReason inactiveReason = entry.getInactiveReason();
        assertNotNull(inactiveReason);

        assertNotNull(inactiveReason.getInactiveReasonType());
        assertEquals(InactiveReasonType.MERGED, inactiveReason.getInactiveReasonType());

        assertNotNull(inactiveReason.getMergeDemergeTos());
        assertEquals(1, inactiveReason.getMergeDemergeTos().size());
        assertEquals("P69159", inactiveReason.getMergeDemergeTos().get(0));
    }

    @Test
    void testEntryWithoutUniprotId() throws Exception {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        Tuple2<String, UniProtkbEntry> result = mapper.call("Q00221,            ,merged ,P69160");
        assertNotNull(result);

        assertNotNull(result._1);
        assertEquals("Q00221", result._1);

        assertNotNull(result._2);
        UniProtkbEntry entry = result._2;

        assertNotNull(entry);
        assertNotNull(entry.getPrimaryAccession());
        assertEquals("Q00221", entry.getPrimaryAccession().getValue());
        assertNull(entry.getUniProtkbId());

        EntryInactiveReason inactiveReason = entry.getInactiveReason();
        assertNotNull(inactiveReason);

        assertNotNull(inactiveReason.getInactiveReasonType());
        assertEquals(InactiveReasonType.MERGED, inactiveReason.getInactiveReasonType());

        assertNotNull(inactiveReason.getMergeDemergeTos());
        assertEquals(1, inactiveReason.getMergeDemergeTos().size());
        assertEquals("P69160", inactiveReason.getMergeDemergeTos().get(0));
    }
}
