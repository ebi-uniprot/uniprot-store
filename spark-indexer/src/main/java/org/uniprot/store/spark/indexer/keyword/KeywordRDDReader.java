package org.uniprot.store.spark.indexer.keyword;

import static org.uniprot.store.spark.indexer.util.SparkUtils.getInputReleaseDirPath;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.cv.keyword.KeywordFileReader;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * This class load Keywords to a JavaPairRDD{key=keywordId, value={@link KeywordEntry}}
 *
 * @author lgonzales
 * @since 2020-10-13
 */
public class KeywordRDDReader {

    /** @return JavaPairRDD{key=keywordId, value={@link KeywordEntry}} */
    public static JavaPairRDD<String, KeywordEntry> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig, String releaseName) {
        String releaseInputDir = getInputReleaseDirPath(applicationConfig, releaseName);
        String filePath = releaseInputDir + applicationConfig.getString("keyword.file.path");
        List<String> lines = SparkUtils.readLines(filePath, jsc.hadoopConfiguration());
        KeywordFileReader fileReader = new KeywordFileReader();
        List<KeywordEntry> entries = fileReader.parseLines(lines);

        return (JavaPairRDD<String, KeywordEntry>)
                jsc.parallelize(entries).mapToPair(new KeywordFileMapper());
    }
}
