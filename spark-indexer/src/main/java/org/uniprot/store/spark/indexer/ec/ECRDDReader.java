package org.uniprot.store.spark.indexer.ec;

import static org.uniprot.store.spark.indexer.util.SparkUtils.getInputReleaseMainThreadDirPath;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.cv.ec.ECCache;
import org.uniprot.cv.ec.ECFileReader;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ECRDDReader {

    /** @return JavaPairRDD{key=ecId, value={@link ECEntry}} */
    public static JavaPairRDD<String, ECEntry> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig, String releaseName) {
        String releaseInputDir = getInputReleaseMainThreadDirPath(applicationConfig, releaseName);
        String dirPath = releaseInputDir + applicationConfig.getString("ec.dir.path");
        String ecClassPath = dirPath + File.separator + ECCache.ENZCLASS_TXT;
        List<String> ecClassLines = SparkUtils.readLines(ecClassPath, jsc.hadoopConfiguration());
        ECFileReader.ECClassFileReader ecClassFileReader = new ECFileReader.ECClassFileReader();
        List<ECEntry> entries = new ArrayList<>();
        entries.addAll(ecClassFileReader.parseLines(ecClassLines));
        ecClassLines.clear();

        String ecDatPath = dirPath + File.separator + ECCache.ENZYME_DAT;
        List<String> ecDatLines = SparkUtils.readLines(ecDatPath, jsc.hadoopConfiguration());
        ECFileReader.ECDatFileReader ecDatFileReader = new ECFileReader.ECDatFileReader();
        entries.addAll(ecDatFileReader.parseLines(ecDatLines));
        ecDatLines.clear();
        return (JavaPairRDD<String, ECEntry>)
                jsc.parallelize(entries).mapToPair(new ECFileMapper());
    }
}
