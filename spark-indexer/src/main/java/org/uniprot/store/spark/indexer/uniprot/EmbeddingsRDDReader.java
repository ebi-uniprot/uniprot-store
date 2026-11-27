package org.uniprot.store.spark.indexer.uniprot;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;

import com.typesafe.config.Config;

public class EmbeddingsRDDReader implements PairRDDReader<String, List<Float>> {

    private final JobParameter jobParameter;

    public EmbeddingsRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, List<Float>> load() {
        JavaSparkContext jsc = jobParameter.getSparkContext();
        SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
        Config config = jobParameter.getApplicationConfig();
        String path = config.getString("embeddings.file.path");
        int dimension =
                config.hasPath("embeddings.dimension")
                        ? config.getInt("embeddings.dimension")
                        : 768;

        return spark.read()
                .parquet(path)
                .select("accession", "vector")
                .javaRDD()
                .filter(
                        row ->
                                !row.isNullAt(0)
                                        && !row.isNullAt(1)
                                        && row.getList(1).size() == dimension)
                .mapToPair(
                        row -> {
                            List<Float> vector = new ArrayList<>(dimension);
                            for (Object value : row.<Object>getList(1)) {
                                vector.add(((Number) value).floatValue());
                            }
                            return new scala.Tuple2<>(row.getString(0), vector);
                        });
    }
}
