package org.uniprot.store.spark.indexer.keyword;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseMainThreadDirPath;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.cv.keyword.KeywordFileReader;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * This class load Keywords to a JavaPairRDD{key=keywordId, value={@link KeywordEntry}}
 *
 * @author lgonzales
 * @since 2020-10-13
 */
public class KeywordRDDReader implements PairRDDReader<String, KeywordEntry> {

    private final JobParameter jobParameter;

    public KeywordRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return JavaPairRDD{key=keywordId, value={@link KeywordEntry}} */
    @Override
    public JavaPairRDD<String, KeywordEntry> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        String releaseInputDir =
                getInputReleaseMainThreadDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("keyword.file.path");
        List<String> lines = SparkUtils.readLines(filePath, jsc.hadoopConfiguration());
        KeywordFileReader fileReader = new KeywordFileReader();
        List<KeywordEntry> entries = fileReader.parseLines(lines);

        return jsc.parallelize(entries).mapToPair(new KeywordFileMapper());
    }
}
