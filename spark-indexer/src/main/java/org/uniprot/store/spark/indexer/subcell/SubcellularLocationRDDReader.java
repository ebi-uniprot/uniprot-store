package org.uniprot.store.spark.indexer.subcell;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseMainThreadDirPath;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.cv.subcell.SubcellularLocationFileReader;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2020-01-16
 */
public class SubcellularLocationRDDReader {

    private SubcellularLocationRDDReader() {}

    /** @return JavaPairRDD{key=subcellId, value={@link SubcellularLocationEntry}} */
    public static JavaPairRDD<String, SubcellularLocationEntry> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig, String releaseName) {
        String releaseInputDir = getInputReleaseMainThreadDirPath(applicationConfig, releaseName);
        String filePath = releaseInputDir + applicationConfig.getString("subcell.file.path");
        SubcellularLocationFileReader fileReader = new SubcellularLocationFileReader();
        List<String> lines = SparkUtils.readLines(filePath, jsc.hadoopConfiguration());
        List<SubcellularLocationEntry> entries = fileReader.parseLines(lines);

        return (JavaPairRDD<String, SubcellularLocationEntry>)
                jsc.parallelize(entries).mapToPair(new SubcellularLocationMapper());
    }
}
