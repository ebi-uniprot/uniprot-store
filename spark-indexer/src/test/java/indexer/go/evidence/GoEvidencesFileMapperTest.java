package indexer.go.evidence;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class GoEvidencesFileMapperTest {

    @Test
    void testMapGoEvidencesLineMapper() throws Exception {
        // given
        String line =
                "A0A021WW32\tGO:0000278\tP\tmitotic cell cycle\tECO:0000315\tIMP\tPMID:12573216\tFlyBase";
        GoEvidencesFileMapper mapper = new GoEvidencesFileMapper();

        // when
        Tuple2<String, GoEvidence> tuple = mapper.call(line);

        // then
        assertNotNull(tuple);

        String accession = tuple._1;
        assertNotNull(accession);
        assertEquals("A0A021WW32", accession);

        GoEvidence goEvidence = tuple._2;
        assertNotNull(goEvidence);
        assertEquals("GO:0000278", goEvidence.getGoId());

        Evidence evidence = goEvidence.getEvidence();
        assertNotNull(evidence);
        assertEquals(EvidenceCode.ECO_0000269, evidence.getEvidenceCode());

        assertEquals("PubMed", evidence.getSource().getDatabaseType().getName());
        assertEquals("12573216", evidence.getSource().getId());
    }

    @Test
    void testInvalidMapGoEvidencesLineMapper() throws Exception {
        GoEvidencesFileMapper mapper = new GoEvidencesFileMapper();
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    mapper.call("INVALID DATA");
                },
                "unable to parse line: 'INVALID DATA' in go evidence file");
    }
}
