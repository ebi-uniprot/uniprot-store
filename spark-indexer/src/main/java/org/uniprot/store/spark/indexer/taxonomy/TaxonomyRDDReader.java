package org.uniprot.store.spark.indexer.taxonomy;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;

/**
 * This class is Responsible to load JavaPairRDD{key=taxId, value=TaxonomyEntry}
 *
 * @author lgonzales
 * @since 2019-10-08
 */
public class TaxonomyRDDReader {

    /** @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry} */
    public static JavaPairRDD<String, TaxonomyEntry> load(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        return (JavaPairRDD<String, TaxonomyEntry>)
                loadTaxonomyNodeRow(sparkContext, applicationConfig)
                        .toJavaRDD()
                        .mapToPair(new TaxonomyRowMapper());
    }

    /** @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry} including lineage data */
    public static JavaPairRDD<String, TaxonomyEntry> loadWithLineage(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        JavaPairRDD<String, TaxonomyEntry> taxonomyNode = load(sparkContext, applicationConfig);

        JavaPairRDD<String, List<TaxonomyLineage>> taxonomyLineage =
                TaxonomyLineageReader.load(sparkContext, applicationConfig);

        return (JavaPairRDD<String, TaxonomyEntry>)
                taxonomyNode.join(taxonomyLineage).mapValues(new TaxonomyJoinMapper());
    }

    private static Dataset<Row> loadTaxonomyNodeRow(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        long maxTaxId = getMaxTaxId(sparkContext, applicationConfig);
        int numberPartition =
                Integer.valueOf(applicationConfig.getString("database.taxonomy.partition"));
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();
        return spark.read()
                .format("jdbc")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("url", applicationConfig.getString("database.url"))
                .option("user", applicationConfig.getString("database.user.name"))
                .option("password", applicationConfig.getString("database.password"))
                .option("dbtable", "taxonomy.v_public_node")
                .option("fetchsize", 5000L)
                .option("numPartitions", numberPartition)
                .option("partitionColumn", "TAX_ID")
                .option("lowerBound", 1)
                .option("upperBound", maxTaxId)
                .load();
    }

    static int getMaxTaxId(JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();

        Dataset<Row> max =
                spark.read()
                        .format("jdbc")
                        .option("driver", "oracle.jdbc.driver.OracleDriver")
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
