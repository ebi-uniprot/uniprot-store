package org.uniprot.store.spark.indexer.taxonomy.reader;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author lgonzales
 * @since 30/05/2020
 */
class TaxonomyUtil {

    private TaxonomyUtil() {}

    static int getMaxTaxId(JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();

        Dataset<Row> max =
                spark.read()
                        .format("jdbc")
                        .option("driver", applicationConfig.getString("database.driver"))
                        .option("url", applicationConfig.getString("database.url"))
                        .option("user", applicationConfig.getString("database.user.name"))
                        .option("password", applicationConfig.getString("database.password"))
                        .option(
                                "query",
                                "SELECT MAX(TAX_ID) AS MAX_TAX_ID FROM TAXONOMY.V_PUBLIC_NODE")
                        .load();

        Row result = max.head();
        return result.getDecimal(result.fieldIndex("MAX_TAX_ID")).intValue();
    }
}
