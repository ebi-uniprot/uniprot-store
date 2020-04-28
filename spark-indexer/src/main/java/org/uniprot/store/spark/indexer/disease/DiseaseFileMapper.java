package org.uniprot.store.spark.indexer.disease;

import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.disease.DiseaseFileReader;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

/**
 * This class map a humdisease string lines of an DiseaseEntry Entry To a Tuple2{key=diseaseId,
 * value={@link DiseaseEntry}}
 *
 * @author lgonzales
 * @since 2019-11-14
 */
@Slf4j
public class DiseaseFileMapper implements PairFunction<String, String, DiseaseEntry> {

    private static final long serialVersionUID = 7494631073619296441L;

    /**
     * @param diseaseLines humdisease string lines of an DiseaseEntry Entry
     * @return a Tuple2{key=diseaseId, value={@link DiseaseEntry}}
     * @throws Exception
     */
    @Override
    public Tuple2<String, DiseaseEntry> call(String diseaseLines) throws Exception {
        diseaseLines = "_________\n" + diseaseLines;
        DiseaseFileReader fileReader = new DiseaseFileReader();
        List<String> diseaseLineList = Arrays.asList(diseaseLines.split("\n"));
        List<DiseaseEntry> diseases = fileReader.parseLines(diseaseLineList);
        if (Utils.notNullNotEmpty(diseases)) {
            DiseaseEntry disease = diseases.get(0);
            return new Tuple2<String, DiseaseEntry>(disease.getName(), disease);
        } else {
            log.info("ERROR PARSING DiseaseFileMapper WITH LINES: " + diseaseLines);
            throw new SparkIndexException(
                    "ERROR PARSING DiseaseFileMapper WITH LINES: " + diseaseLines);
        }
    }
}
