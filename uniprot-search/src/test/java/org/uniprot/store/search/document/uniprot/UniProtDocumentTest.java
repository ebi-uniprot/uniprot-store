package org.uniprot.store.search.document.uniprot;

import org.apache.solr.client.solrj.beans.Field;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 02/05/2020
 */
class UniProtDocumentTest {

    @Test
    void testEquals() {
        UniProtDocument doc = getCompleteDocument(new Date());
        UniProtDocument doc2 = getCompleteDocument(new Date());
        assertEquals(doc, doc2);
        assertEquals(doc.hashCode(), doc2.hashCode());
    }

    @Test
    void testToString() {
        UniProtDocument doc = getCompleteDocument(new Date());
        assertTrue(doc.toString().startsWith("UniProtDocument{accession='P21802'"));
    }

    @Test
    void testGetDocumentId() {
        UniProtDocument doc = getCompleteDocument(new Date());
        assertEquals("P21802", doc.getDocumentId());
    }


    private UniProtDocument getCompleteDocument(Date date) {
        UniProtDocument doc =new UniProtDocument();
        doc.accession = "P21802";
        doc.id = "1";
        doc.idDefault = "1";
        doc.reviewed = true;
        doc.proteinsNamesSort = "1";
        doc.lastModified = date;
        doc.firstCreated = date;
        doc.sequenceUpdated = date;
        doc.geneNamesSort = "1";
        doc.organismSort = "1";
        doc.organismTaxId = 1;
        doc.modelOrganism = "1";
        doc.otherOrganism = "1";
        doc.proteinExistence = "1";
        doc.fragment = true;
        doc.precursor = true;
        doc.active = true;
        doc.d3structure = false;
        doc.seqMass = 1;
        doc.seqLength = 1;
        doc.seqAA = "1";
        doc.score = 1;
        doc.avroBinary = "1".getBytes();
        doc.avroBin = "1";
        doc.avroJson = "1";
        doc.inactiveReason = "1";
        doc.isIsoform = false;
        doc.unirefCluster50 = "1";
        doc.unirefCluster90 = "1";
        doc.unirefCluster100 = "1";
        doc.uniparc = "1";
        return doc;
    }
}