package org.uniprot.store.spark.indexer.disease;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;

/**
 * This class load Diseases to a JavaPairRDD{key=diseaseId, value={@link DiseaseEntry}}
 *
 * @author lgonzales
 * @since 2019-10-13
 */
public class DiseaseRDDReader implements PairRDDReader<String, DiseaseEntry> {

    private final JobParameter jobParameter;
    private static final String SPLITTER = "\n//\n";

    public DiseaseRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return JavaPairRDD{key=diseaseId, value={@link DiseaseEntry}} */
    @Override
    public JavaPairRDD<String, DiseaseEntry> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("disease.file.path");
        jobParameter
                .getSparkContext()
                .hadoopConfiguration()
                .set("textinputformat.record.delimiter", SPLITTER);

        return jobParameter
                .getSparkContext()
                .textFile(filePath)
                .map(e -> "______\n" + e + SPLITTER)
                .mapToPair(new DiseaseFileMapper());
    }
}
