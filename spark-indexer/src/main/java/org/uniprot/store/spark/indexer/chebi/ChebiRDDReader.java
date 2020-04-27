package org.uniprot.store.spark.indexer.chebi;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.chebi.ChebiEntry;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiRDDReader {

    private ChebiRDDReader() {}

    /** @return JavaPairRDD{key=chebiId, value={@link ChebiEntry}} */
    public static JavaPairRDD<String, ChebiEntry> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig, String releaseName) {
        String releaseInputDir = getInputReleaseDirPath(applicationConfig, releaseName);
        String filePath = releaseInputDir + applicationConfig.getString("chebi.file.path");

        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");

        return jsc.textFile(filePath)
                .filter(
                        input ->
                                !input.startsWith("format-version")
                                        && !input.startsWith("[Typedef]"))
                .mapToPair(new ChebiFileMapper());
    }
}
