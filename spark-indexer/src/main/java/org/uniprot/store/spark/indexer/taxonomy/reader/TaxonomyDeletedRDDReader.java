package org.uniprot.store.spark.indexer.taxonomy.reader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyDeletedRowMapper;

import com.typesafe.config.Config;

public class TaxonomyDeletedRDDReader implements RDDReader<TaxonomyDocument> {

    private final JobParameter jobParameter;

    public TaxonomyDeletedRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * @return return a JavaRDD{TaxonomyDocument}
     */
    public JavaRDD<TaxonomyDocument> load() {
        return loadNodeRow().toJavaRDD().map(new TaxonomyDeletedRowMapper());
    }

    private Dataset<Row> loadNodeRow() {
        JavaSparkContext sparkContext = jobParameter.getSparkContext();
        Config applicationConfig = jobParameter.getApplicationConfig();
        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();
        return spark.read()
                .format("jdbc")
                .option("driver", applicationConfig.getString("database.driver"))
                .option("url", applicationConfig.getString("database.url"))
                .option("user", applicationConfig.getString("database.user.name"))
                .option("password", applicationConfig.getString("database.password"))
                .option("dbtable", "taxonomy.v_public_deleted")
                .option("fetchsize", 1000L)
                .load();
    }
}
