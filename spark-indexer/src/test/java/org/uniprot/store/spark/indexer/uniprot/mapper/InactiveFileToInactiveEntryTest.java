package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.DeletedReason;
import org.uniprot.core.uniprotkb.EntryInactiveReason;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class InactiveFileToInactiveEntryTest {

    @Test
    void testDeletedEntryWithoutReason() throws Exception {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        Tuple2<String, UniProtKBEntry> result = mapper.call("I8FBX0,I8FBX0_MYCAB,deleted,-, ");
        validateDeleted(result, "I8FBX0", "I8FBX0_MYCAB", DeletedReason.UNKNOWN);
    }

    @Test
    void testDeletedEntryWithMultipleIdReason() throws Exception {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        Tuple2<String, UniProtKBEntry> result = mapper.call("I8FBX1,I8FBX1_MYCAB,deleted,-,8");
        validateDeleted(result, "I8FBX1", "I8FBX1_MYCAB", DeletedReason.SOURCE_DELETION);
    }

    @Test
    void testDeletedEntryWithSingleIdReason() throws Exception {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        Tuple2<String, UniProtKBEntry> result = mapper.call("I8FBX1,I8FBX1_MYCAB,deleted,-,13");
        validateDeleted(result, "I8FBX1", "I8FBX1_MYCAB", DeletedReason.PROTEOME_REDUNDANCY);
    }

    @Test
    void testDeletedEntryWithInvalidReason() {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> mapper.call("I8FBX9,I8FBX9_MYCAB,deleted,-,XX"));
        assertEquals("The DeletedReason id 'XX' doesn't exist.", error.getMessage());
    }

    @Test
    void testMergedEntry() throws Exception {
        InactiveFileToInactiveEntry mapper = new InactiveFileToInactiveEntry();

        Tuple2<String, UniProtKBEntry> result = mapper.call("Q00220,SOMA_HYPMO  ,merged ,P69159");
        validateTupleValues(result, "Q00220");

        UniProtKBEntry entry = result._2;
        validateAccessionAndUniProtKBId("Q00220", "SOMA_HYPMO", entry);

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

        Tuple2<String, UniProtKBEntry> result = mapper.call("Q00221,            ,merged ,P69160");
        validateTupleValues(result, "Q00221");

        UniProtKBEntry entry = result._2;
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

    private static void validateDeleted(
            Tuple2<String, UniProtKBEntry> result,
            String I8FBX1,
            String I8FBX1_MYCAB,
            DeletedReason pdbDeletion) {
        validateTupleValues(result, I8FBX1);

        UniProtKBEntry entry = result._2;
        validateAccessionAndUniProtKBId(I8FBX1, I8FBX1_MYCAB, entry);

        EntryInactiveReason inactiveReason = entry.getInactiveReason();
        assertNotNull(inactiveReason);

        assertNotNull(inactiveReason.getInactiveReasonType());
        assertEquals(InactiveReasonType.DELETED, inactiveReason.getInactiveReasonType());
        assertEquals(pdbDeletion, inactiveReason.getDeletedReason());

        assertNotNull(inactiveReason.getMergeDemergeTos());
        assertTrue(inactiveReason.getMergeDemergeTos().isEmpty());
    }

    private static void validateAccessionAndUniProtKBId(
            String I8FBX1, String I8FBX1_MYCAB, UniProtKBEntry entry) {
        assertNotNull(entry);
        assertNotNull(entry.getPrimaryAccession());
        assertEquals(I8FBX1, entry.getPrimaryAccession().getValue());
        assertNotNull(entry.getUniProtkbId());
        assertEquals(I8FBX1_MYCAB, entry.getUniProtkbId().getValue());
    }

    private static void validateTupleValues(Tuple2<String, UniProtKBEntry> result, String I8FBX1) {
        assertNotNull(result);

        assertNotNull(result._1);
        assertEquals(I8FBX1, result._1);

        assertNotNull(result._2);
    }
}
