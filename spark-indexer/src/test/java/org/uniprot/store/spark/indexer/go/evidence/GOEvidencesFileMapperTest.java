package org.uniprot.store.spark.indexer.go.evidence;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class GOEvidencesFileMapperTest {

    @Test
    void testMapGoEvidencesLineMapper() throws Exception {
        // given
        String line =
                "A0A021WW32\tGO:0000278\tP\tmitotic cell cycle\tECO:0000315\tIMP\tPMID:12573216\tFlyBase";
        GOEvidencesFileMapper mapper = new GOEvidencesFileMapper();

        // when
        Tuple2<String, GOEvidence> tuple = mapper.call(line);

        // then
        assertNotNull(tuple);

        String accession = tuple._1;
        assertNotNull(accession);
        assertEquals("A0A021WW32", accession);

        GOEvidence goEvidence = tuple._2;
        assertNotNull(goEvidence);
        assertEquals("GO:0000278", goEvidence.getGoId());

        Evidence evidence = goEvidence.getEvidence();
        assertNotNull(evidence);
        assertEquals(EvidenceCode.ECO_0000269, evidence.getEvidenceCode());

        assertEquals("PubMed", evidence.getEvidenceCrossReference().getDatabase().getName());
        assertEquals("12573216", evidence.getEvidenceCrossReference().getId());
    }

    @Test
    void testInvalidMapGoEvidencesLineMapper() throws Exception {
        GOEvidencesFileMapper mapper = new GOEvidencesFileMapper();
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    mapper.call("INVALID DATA");
                },
                "unable to parse line: 'INVALID DATA' in go evidence file");
    }
}
