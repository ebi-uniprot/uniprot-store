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
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;

/**
 * This class is Responsible to load JavaPairRDD{key=taxId, value=TaxonomyEntry}
 *
 * @author lgonzales
 * @since 2019-10-08
 */
public class TaxonomyRDDReader implements PairRDDReader<String, TaxonomyEntry> {

    private final JobParameter jobParameter;
    private final TaxonomyLineageReader taxonomyLineageReader;

    public TaxonomyRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        this.taxonomyLineageReader = new TaxonomyLineageReader(jobParameter, false);
    }

    /** @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry} */
    public JavaPairRDD<String, TaxonomyEntry> load() {
        return loadTaxonomyNodeRow().toJavaRDD().mapToPair(new TaxonomyRowMapper());
    }

    /** @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry} including lineage data */
    public JavaPairRDD<String, TaxonomyEntry> loadWithLineage() {
        JavaPairRDD<String, TaxonomyEntry> taxonomyNode = load();

        JavaPairRDD<String, List<TaxonomyLineage>> taxonomyLineage = taxonomyLineageReader.load();

        return taxonomyNode.join(taxonomyLineage).mapValues(new TaxonomyJoinMapper());
    }

    private Dataset<Row> loadTaxonomyNodeRow() {
        JavaSparkContext sparkContext = jobParameter.getSparkContext();
        ResourceBundle applicationConfig = jobParameter.getApplicationConfig();
        long maxTaxId = TaxonomyUtil.getMaxTaxId(sparkContext, applicationConfig);
        int numberPartition =
                Integer.parseInt(applicationConfig.getString("database.taxonomy.partition"));
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();
        return spark.read()
                .format("jdbc")
                .option("driver", applicationConfig.getString("database.driver"))
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
}
