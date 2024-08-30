package org.uniprot.store.spark.indexer.uniparc;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcSequenceSourceMapper;

import com.typesafe.config.Config;

public class UniParcSequenceSourceMapperRDDTupleReader
        implements PairRDDReader<String, Set<String>> {

    private final JobParameter jobParameter;

    public UniParcSequenceSourceMapperRDDTupleReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, Set<String>> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("uniparc.source.mapper.file.path");
        return spark.read()
                .textFile(filePath)
                .toJavaRDD()
                .mapToPair(new UniParcSequenceSourceMapper())
                .aggregateByKey(new HashSet<>(), aggregate(), aggregate());
    }

    private Function2<Set<String>, Set<String>, Set<String>> aggregate() {
        return (s1, s2) -> {
            s1.addAll(s2);
            return s1;
        };
    }
}
