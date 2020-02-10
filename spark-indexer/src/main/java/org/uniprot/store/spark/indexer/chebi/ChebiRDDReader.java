package org.uniprot.store.spark.indexer.chebi;

import java.util.ResourceBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.cv.chebi.Chebi;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiRDDReader {

    /** @return JavaPairRDD{key=chebiId, value={@link Chebi}} */
    public static JavaPairRDD<String, Chebi> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String filePath = applicationConfig.getString("chebi.file.path");

        Configuration conf = new Configuration(jsc.hadoopConfiguration());
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");
        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();

        return (JavaPairRDD<String, Chebi>)
                jsc.textFile(filePath)
                        .filter(
                                input ->
                                        !input.startsWith("format-version")
                                                && !input.startsWith("[Typedef]"))
                        .mapToPair(new ChebiFileMapper());
    }
}
