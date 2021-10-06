package org.uniprot.store.spark.indexer.taxonomy.reader;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.mapper.TaxonomyStrainsRowMapper;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.Strain;

import java.util.ResourceBundle;

public class TaxonomyStrainsRDDReader implements PairRDDReader<String, Strain> {

    private final JobParameter jobParameter;

    public TaxonomyStrainsRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return return a JavaPairRDD{key=taxId, value=TaxonomyEntry} */
    public JavaPairRDD<String, Strain> load() {
        return loadNodeRow().toJavaRDD().mapToPair(new TaxonomyStrainsRowMapper());
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
                .option("dbtable", "taxonomy.v_public_strain")
                .option("fetchsize", 1000L)
                .load();
    }
}
