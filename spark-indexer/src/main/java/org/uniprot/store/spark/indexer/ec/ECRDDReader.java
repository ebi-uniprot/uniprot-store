package org.uniprot.store.spark.indexer.ec;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseMainThreadDirPath;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.cv.ec.ECCache;
import org.uniprot.cv.ec.ECFileReader;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ECRDDReader implements PairRDDReader<String, ECEntry> {

    private final JobParameter jobParameter;

    public ECRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return JavaPairRDD{key=ecId, value={@link ECEntry}} */
    @Override
    public JavaPairRDD<String, ECEntry> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        String releaseInputDir =
                getInputReleaseMainThreadDirPath(config, jobParameter.getReleaseName());
        String dirPath = releaseInputDir + config.getString("ec.dir.path");
        String ecClassPath = dirPath + File.separator + ECCache.ENZCLASS_TXT;
        List<String> ecClassLines = SparkUtils.readLines(ecClassPath, jsc.hadoopConfiguration());

        ECFileReader.ECClassFileReader ecClassFileReader = new ECFileReader.ECClassFileReader();
        List<ECEntry> entries = new ArrayList<>(ecClassFileReader.parseLines(ecClassLines));
        ecClassLines.clear();

        String ecDatPath = dirPath + File.separator + ECCache.ENZYME_DAT;
        List<String> ecDatLines = SparkUtils.readLines(ecDatPath, jsc.hadoopConfiguration());
        ECFileReader.ECDatFileReader ecDatFileReader = new ECFileReader.ECDatFileReader();
        entries.addAll(ecDatFileReader.parseLines(ecDatLines));
        ecDatLines.clear();
        return jsc.parallelize(entries).mapToPair(new ECFileMapper());
    }
}
