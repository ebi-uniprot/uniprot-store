package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtIdTrackerMapper;

import com.typesafe.config.Config;

public class UniProtIdTrackerRDDReader implements PairRDDReader<String, Set<String>> {

    private final JobParameter jobParameter;

    public UniProtIdTrackerRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, Set<String>> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("uniprot.idtracker.file.path");
        Function2<Set<String>, Set<String>, Set<String>> aggregate = getAggregateFunction();
        return spark.read()
                .textFile(filePath)
                .toJavaRDD()
                .mapToPair(new UniProtIdTrackerMapper())
                .aggregateByKey(new HashSet<>(), aggregate, aggregate);
    }

    private Function2<Set<String>, Set<String>, Set<String>> getAggregateFunction() {
        return (s1, s2) -> {
            s1.addAll(s2);
            return s1;
        };
    }
}
