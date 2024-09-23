package org.uniprot.store.spark.indexer.taxonomy.reader;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyOtherNamesRowMapper;

import com.typesafe.config.Config;

import static org.uniprot.store.spark.indexer.taxonomy.reader.TaxReaderConstants.READ;

public class TaxonomyOtherNamesRDDReader implements PairRDDReader<String, TaxonomyEntry> {

    private final JobParameter jobParameter;

    public TaxonomyOtherNamesRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry}
     */
    public JavaPairRDD<String, TaxonomyEntry> load() {
        return loadNodeRow().toJavaRDD().mapToPair(new TaxonomyOtherNamesRowMapper());
    }

    private Dataset<Row> loadNodeRow() {
        JavaSparkContext sparkContext = jobParameter.getSparkContext();
        Config applicationConfig = jobParameter.getApplicationConfig();
        String taxDb = jobParameter.getTaxDb();
        boolean isReadDb = READ.equals(taxDb);
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();
        return spark.read()
                .format("jdbc")
                .option("driver", applicationConfig.getString("database.driver"))
                .option("url", isReadDb ? applicationConfig.getString("database.read.url") : applicationConfig.getString("database.fly.url"))
                .option("user", isReadDb ? applicationConfig.getString("database.read.user.name") : applicationConfig.getString("database.fly.user.name"))
                .option("password", isReadDb ? applicationConfig.getString("database.read.password"):applicationConfig.getString("database.fly.password"))
                .option("dbtable", "taxonomy.v_public_name")
                .option("fetchsize", 1000L)
                .load();
    }
}
