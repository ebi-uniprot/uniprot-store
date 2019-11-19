package indexer.go.evidence;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-13
 */
@Slf4j
public class GoEvidencesRDDReader {

    public static JavaPairRDD<String, Iterable<GoEvidence>> load(SparkConf sparkConf, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        return (JavaPairRDD<String, Iterable<GoEvidence>>) spark.read()
                .textFile(applicationConfig.getString("go.evidence.file.path"))
                .toJavaRDD()
                .mapToPair(new GoEvidencesFileMapper())
                .groupByKey();
    }

}
