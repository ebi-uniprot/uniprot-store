package org.uniprot.store.spark.indexer.go.evidence;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.cv.evidence.EvidenceHelper;

import scala.Tuple2;

/**
 * This class map extended GO Evidences string line To a Tuple2{key=uniprot accession, value={@link
 * GOEvidence}}
 *
 * @author lgonzales
 * @since 2019-11-14
 */
@Slf4j
public class GOEvidencesFileMapper implements PairFunction<String, String, GOEvidence> {

    private static final long serialVersionUID = 7265825845507683822L;

    /**
     * @param line Go Evidences string line
     * @return Tuple2{key=uniprot accession, value={@link GOEvidence}}
     */
    @Override
    public Tuple2<String, GOEvidence> call(String line) throws Exception {
        String[] splitedLine = line.split("\t");
        if (splitedLine.length >= 7) {
            String accession = splitedLine[0];
            String goId = splitedLine[1];
            String evidenceValue = splitedLine[6].replace("PMID", "ECO:0000269|PubMed");
            Evidence evidence = EvidenceHelper.parseEvidenceLine(evidenceValue);
            return new Tuple2<>(accession, new GOEvidence(goId, evidence));
        } else {
            throw new IllegalArgumentException(
                    "unable to parse line: '" + line + "' in go evidence file");
        }
    }
}
