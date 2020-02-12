package org.uniprot.store.spark.indexer.disease;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.disease.DiseaseEntry;

/**
 * This class load Diseases to a JavaPairRDD{key=diseaseId, value={@link DiseaseEntry}}
 *
 * @author lgonzales
 * @since 2019-10-13
 */
public class DiseaseRDDReader {

    private static final String SPLITTER = "\n//\n";

    /** @return JavaPairRDD{key=diseaseId, value={@link DiseaseEntry}} */
    public static JavaPairRDD<String, DiseaseEntry> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String filePath = applicationConfig.getString("disease.file.path");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);

        return (JavaPairRDD<String, DiseaseEntry>)
                jsc.textFile(filePath)
                        .map(e -> "______\n" + e + SPLITTER)
                        .mapToPair(new DiseaseFileMapper());
    }
}
