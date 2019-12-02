package indexer.disease;

import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.disease.Disease;
import org.uniprot.core.cv.disease.DiseaseFileReader;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
@Slf4j
public class DiseaseFileMapper implements PairFunction<String, String, Disease> {

    private static final long serialVersionUID = 7494631073619296441L;

    @Override
    public Tuple2<String, Disease> call(String diseaseLines) throws Exception {
        diseaseLines = "_________\n" + diseaseLines;
        DiseaseFileReader fileReader = new DiseaseFileReader();
        List<String> diseaseLineList = Arrays.asList(diseaseLines.split("\n"));
        List<Disease> diseases = fileReader.parseLines(diseaseLineList);
        if (Utils.notNullOrEmpty(diseases)) {
            Disease disease = diseases.get(0);
            return new Tuple2<String, Disease>(disease.getId(), disease);
        } else {
            log.info("ERROR PARSING DiseaseFileMapper WITH LINES: " + diseaseLines);
            throw new RuntimeException(
                    "ERROR PARSING DiseaseFileMapper WITH LINES: " + diseaseLines);
        }
    }
}
