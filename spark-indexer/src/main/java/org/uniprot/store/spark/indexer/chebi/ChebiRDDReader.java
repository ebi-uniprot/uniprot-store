package org.uniprot.store.spark.indexer.chebi;

import java.util.ResourceBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.cv.chebi.ChebiEntry;

/**
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiRDDReader {

    /** @return JavaPairRDD{key=chebiId, value={@link ChebiEntry}} */
    public static JavaPairRDD<String, ChebiEntry> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String filePath = applicationConfig.getString("chebi.file.path");

        Configuration conf = new Configuration(jsc.hadoopConfiguration());
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");
        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();

        return (JavaPairRDD<String, ChebiEntry>)
                jsc.textFile(filePath)
                        .filter(
                                input ->
                                        !input.startsWith("format-version")
                                                && !input.startsWith("[Typedef]"))
                        .mapToPair(new ChebiFileMapper());
    }
}
