package org.uniprot.store.spark.indexer.chebi;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiRDDReader {

    private ChebiRDDReader() {}

    /** @return JavaPairRDD{key=chebiId, value={@link ChebiEntry}} */
    public static JavaPairRDD<String, ChebiEntry> load(JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("chebi.file.path");

        jobParameter
                .getSparkContext()
                .hadoopConfiguration()
                .set("textinputformat.record.delimiter", "\n\n");

        return jobParameter
                .getSparkContext()
                .textFile(filePath)
                .filter(
                        input ->
                                !input.startsWith("format-version")
                                        && !input.startsWith("[Typedef]"))
                .mapToPair(new ChebiFileMapper());
    }
}
