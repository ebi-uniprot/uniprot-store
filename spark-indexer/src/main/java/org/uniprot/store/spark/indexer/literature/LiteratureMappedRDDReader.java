package org.uniprot.store.spark.indexer.literature;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.store.spark.indexer.common.JobParameter;

/**
 * Class responsible to load JavaPairRDD from PIR mapped files.
 *
 * @author lgonzales
 * @since 2019-12-02
 */
public class LiteratureMappedRDDReader {

    private LiteratureMappedRDDReader() {}

    /**
     * load LiteratureMappedReference to a JavaPairRDD
     *
     * @return JavaPairRDD{key=PubmedId, value=Iterable of LiteratureMappedReference}
     */
    public static JavaPairRDD<String, Iterable<LiteratureMappedReference>> load(
            JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String literaturePath = releaseInputDir + config.getString("literature.map.file.path");
        return (JavaPairRDD<String, Iterable<LiteratureMappedReference>>)
                spark.read()
                        .textFile(literaturePath)
                        .toJavaRDD()
                        .mapToPair(new LiteratureMappedFileMapper())
                        .groupByKey();
    }

    /**
     * load PubmedIds to a JavaPairRDD
     *
     * @return JavaPairRDD{key=Uniprot accession, value=Iterable of PubmedId}
     */
    public static JavaPairRDD<String, Iterable<String>> loadAccessionPubMedRDD(
            JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String literaturePath = releaseInputDir + config.getString("literature.map.file.path");
        return (JavaPairRDD<String, Iterable<String>>)
                spark.read()
                        .textFile(literaturePath)
                        .toJavaRDD()
                        .mapToPair(new LiteraturePubmedFileMapper())
                        .groupByKey();
    }
}
