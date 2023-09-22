package org.uniprot.store.spark.indexer.literature;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.literature.mapper.LiteratureMappedFileMapper;
import org.uniprot.store.spark.indexer.literature.mapper.LiteraturePubmedFileMapper;

import scala.Tuple2;

import com.typesafe.config.Config;

/**
 * Class responsible to load JavaPairRDD from PIR mapped files.
 *
 * @author lgonzales
 * @since 2019-12-02
 */
public class LiteratureMappedRDDReader
        implements PairRDDReader<String, Iterable<LiteratureMappedReference>> {

    private final JobParameter jobParameter;

    public LiteratureMappedRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * load LiteratureMappedReference to a JavaPairRDD
     *
     * @return JavaPairRDD{key=PubmedId, value=Iterable of LiteratureMappedReference}
     */
    @Override
    public JavaPairRDD<String, Iterable<LiteratureMappedReference>> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String literaturePath = releaseInputDir + config.getString("literature.map.file.path");
        return spark.read()
                .textFile(literaturePath)
                .toJavaRDD()
                .mapToPair(new LiteratureMappedFileMapper())
                .groupByKey();
    }

    /**
     * load PubmedIds to a JavaPairRDD
     *
     * @return JavaPairRDD{key=Uniprot accession, value=Iterable of [sourceType, PubmedId]}
     */
    public JavaPairRDD<String, Iterable<Tuple2<String, String>>> loadAccessionPubMedRDD() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String literaturePath = releaseInputDir + config.getString("literature.map.file.path");
        return spark.read()
                .textFile(literaturePath)
                .toJavaRDD()
                .mapToPair(new LiteraturePubmedFileMapper())
                .groupByKey();
    }
}
