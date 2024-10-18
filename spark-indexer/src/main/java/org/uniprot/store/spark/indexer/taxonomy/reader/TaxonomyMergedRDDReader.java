package org.uniprot.store.spark.indexer.taxonomy.reader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyMergedRowMapper;

import com.typesafe.config.Config;

public class TaxonomyMergedRDDReader implements RDDReader<TaxonomyDocument> {

    private final JobParameter jobParameter;

    public TaxonomyMergedRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * @return return a JavaRDD{TaxonomyDocument}
     */
    public JavaRDD<TaxonomyDocument> load() {
        return loadNodeRow().toJavaRDD().map(new TaxonomyMergedRowMapper());
    }

    private Dataset<Row> loadNodeRow() {
        JavaSparkContext sparkContext = jobParameter.getSparkContext();
        Config applicationConfig = jobParameter.getApplicationConfig();
        String taxDb = jobParameter.getTaxDb().getName();
        String databasePropertyPrefix = "database." + taxDb;
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();
        return spark.read()
                .format("jdbc")
                .option("driver", applicationConfig.getString("database.driver"))
                .option("url", applicationConfig.getString(databasePropertyPrefix + ".url"))
                .option("user", applicationConfig.getString(databasePropertyPrefix + ".user.name"))
                .option("password", applicationConfig.getString(databasePropertyPrefix + ".password"))
                .option("dbtable", "taxonomy.v_public_merged")
                .option("fetchsize", 1000L)
                .load();
    }
}
