package indexer.literature;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.literature.LiteratureMappedReference;

/**
 * @author lgonzales
 * @since 2019-12-02
 */
public class LiteratureMappedRDDReader {

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
}
