package org.uniprot.store.spark.indexer.go.evidence;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 16/05/2020
 */
class GOEvidenceTest {

    @Test
    void testEquals() {
        GOEvidence evidence = new GOEvidence("id", null);
        GOEvidence evidence2 = new GOEvidence("id", null);
        assertEquals(evidence, evidence2);
    }

    @Test
    void testHashCode() {
        GOEvidence evidence = new GOEvidence("id", null);
        GOEvidence evidence2 = new GOEvidence("id", null);
        assertEquals(evidence.hashCode(), evidence2.hashCode());
    }

    @Test
    void testToString() {
        GOEvidence evidence = new GOEvidence("id", null);
        assertEquals("GOEvidence(goId=id, evidence=null)", evidence.toString());
    }
}