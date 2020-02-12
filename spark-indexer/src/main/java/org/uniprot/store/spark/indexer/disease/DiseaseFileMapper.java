package org.uniprot.store.spark.indexer.disease;

import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.disease.Disease;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.disease.DiseaseFileReader;

import scala.Tuple2;

/**
 * This class map a humdisease string lines of an Disease Entry To a Tuple2{key=diseaseId,
 * value={@link Disease}}
 *
 * @author lgonzales
 * @since 2019-11-14
 */
@Slf4j
public class DiseaseFileMapper implements PairFunction<String, String, Disease> {

    private static final long serialVersionUID = 7494631073619296441L;

    /**
     * @param diseaseLines humdisease string lines of an Disease Entry
     * @return a Tuple2{key=diseaseId, value={@link Disease}}
     * @throws Exception
     */
    @Override
    public Tuple2<String, Disease> call(String diseaseLines) throws Exception {
        diseaseLines = "_________\n" + diseaseLines;
        DiseaseFileReader fileReader = new DiseaseFileReader();
        List<String> diseaseLineList = Arrays.asList(diseaseLines.split("\n"));
        List<Disease> diseases = fileReader.parseLines(diseaseLineList);
        if (Utils.notNullNotEmpty(diseases)) {
            Disease disease = diseases.get(0);
            return new Tuple2<String, Disease>(disease.getId(), disease);
        } else {
            log.info("ERROR PARSING DiseaseFileMapper WITH LINES: " + diseaseLines);
            throw new RuntimeException(
                    "ERROR PARSING DiseaseFileMapper WITH LINES: " + diseaseLines);
        }
    }
}
