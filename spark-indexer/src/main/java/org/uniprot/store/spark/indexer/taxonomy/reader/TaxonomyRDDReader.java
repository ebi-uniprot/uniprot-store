package org.uniprot.store.spark.indexer.taxonomy.reader;

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
import org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyJoinMapper;
import org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyRowMapper;

/**
 * This class is Responsible to load JavaPairRDD{key=taxId, value=TaxonomyEntry}
 *
 * @author lgonzales
 * @since 2019-10-08
 */
public class TaxonomyRDDReader implements PairRDDReader<String, TaxonomyEntry> {

    private final JobParameter jobParameter;
    private final TaxonomyLineageReader taxonomyLineageReader;
    private final boolean withLineage;

    public TaxonomyRDDReader(JobParameter jobParameter, boolean withLineage) {
        this.jobParameter = jobParameter;
        this.withLineage = withLineage;
        this.taxonomyLineageReader = new TaxonomyLineageReader(jobParameter, false);
    }

    /** @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry} */
    public JavaPairRDD<String, TaxonomyEntry> load() {
        JavaPairRDD<String, TaxonomyEntry> taxonomy =
                loadTaxonomyNodeRow().toJavaRDD().mapToPair(new TaxonomyRowMapper());
        if (withLineage) {
            JavaPairRDD<String, List<TaxonomyLineage>> taxonomyLineage = loadTaxonomyLineage();
            return taxonomy.join(taxonomyLineage).mapValues(new TaxonomyJoinMapper());
        } else {
            return taxonomy;
        }
    }

    protected JavaPairRDD<String, List<TaxonomyLineage>> loadTaxonomyLineage() {
        return taxonomyLineageReader.load();
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
