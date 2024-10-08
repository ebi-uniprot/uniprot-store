package org.uniprot.store.spark.indexer.taxonomy.reader;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.typesafe.config.Config;
import org.uniprot.store.spark.indexer.common.JobParameter;

import static org.uniprot.store.spark.indexer.taxonomy.reader.TaxReaderConstants.READ;

/**
 * @author lgonzales
 * @since 30/05/2020
 */
class TaxonomyUtil {

    private TaxonomyUtil() {}

    static int getMaxTaxId(JavaSparkContext sparkContext, Config applicationConfig, JobParameter jobParameter) {
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();
        String taxDb = jobParameter.getTaxDb().getName();
        String databasePropertyPrefix = "database." + taxDb;

        Dataset<Row> max =
                spark.read()
                        .format("jdbc")
                        .option("driver", applicationConfig.getString("database.driver"))
                        .option("url", applicationConfig.getString(databasePropertyPrefix + ".url"))
                        .option("user", applicationConfig.getString(databasePropertyPrefix + ".user.name"))
                        .option("password", applicationConfig.getString(databasePropertyPrefix + ".password"))
                        .option(
                                "query",
                                "SELECT MAX(TAX_ID) AS MAX_TAX_ID FROM TAXONOMY.V_PUBLIC_NODE")
                        .load();

        Row result = max.head();
        return result.getDecimal(result.fieldIndex("MAX_TAX_ID")).intValue();
    }
}
