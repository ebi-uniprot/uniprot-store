package indexer.uniprot;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import indexer.uniprot.mapper.InactiveFileToInactiveEntry;

/**
 * @author lgonzales
 * @since 2019-12-02
 */
public class InactiveUniprotRDDTupleReader {

    public static JavaPairRDD<String, UniProtDocument> load(
            SparkConf sparkConf, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        return (JavaPairRDD<String, UniProtDocument>)
                spark.read()
                        .textFile(applicationConfig.getString("uniprot.inactive.file.path"))
                        .toJavaRDD()
                        .mapToPair(new InactiveFileToInactiveEntry())
                        .mapValues(e -> new UniProtDocument());
        /*                        .aggregateByKey(e --> e,
            inactive -> {
        UniProtDocument document = new UniProtDocument();
        return document; } , (doc1, doc2) -> { return doc1; } );*/
    }
}
