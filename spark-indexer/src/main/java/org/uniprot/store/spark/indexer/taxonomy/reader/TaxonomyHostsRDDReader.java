package org.uniprot.store.spark.indexer.taxonomy.reader;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyHostsRowMapper;

public class TaxonomyHostsRDDReader implements PairRDDReader<String, String> {

    private final JobParameter jobParameter;

    public TaxonomyHostsRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry} */
    public JavaPairRDD<String, String> load() {
        return loadNodeRow().toJavaRDD().mapToPair(new TaxonomyHostsRowMapper());
    }

    private Dataset<Row> loadNodeRow() {
        JavaSparkContext sparkContext = jobParameter.getSparkContext();
        ResourceBundle applicationConfig = jobParameter.getApplicationConfig();
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();
        return spark.read()
                .format("jdbc")
                .option("driver", applicationConfig.getString("database.driver"))
                .option("url", applicationConfig.getString("database.url"))
                .option("user", applicationConfig.getString("database.user.name"))
                .option("password", applicationConfig.getString("database.password"))
                .option("dbtable", "taxonomy.v_public_host")
                .option("fetchsize", 1000L)
                .load();
    }
}
