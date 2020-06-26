package org.uniprot.store.spark.indexer.chebi;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiRDDReader implements PairRDDReader<String, ChebiEntry> {

    private final JobParameter jobParameter;

    public ChebiRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return JavaPairRDD{key=chebiId, value={@link ChebiEntry}} */
    @Override
    public JavaPairRDD<String, ChebiEntry> load() {
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
