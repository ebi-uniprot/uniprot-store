package indexer.literature;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.literature.LiteratureMappedReference;

/**
 * Class responsible to load JavaPairRDD from PIR mapped files.
 *
 * @author lgonzales
 * @since 2019-12-02
 */
public class LiteratureMappedRDDReader {

    /**
     * load LiteratureMappedReference to a JavaPairRDD
     *
     * @return JavaPairRDD{key=PubmedId, value=Iterable of LiteratureMappedReference}
     */
    public static JavaPairRDD<String, Iterable<LiteratureMappedReference>> load(
            SparkConf sparkConf, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        return (JavaPairRDD<String, Iterable<LiteratureMappedReference>>)
                spark.read()
                        .textFile(applicationConfig.getString("literature.map.file.path"))
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
            SparkConf sparkConf, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        return (JavaPairRDD<String, Iterable<String>>)
                spark.read()
                        .textFile(applicationConfig.getString("literature.map.file.path"))
                        .toJavaRDD()
                        .mapToPair(new LiteraturePubmedFileMapper())
                        .groupByKey();
    }
}
