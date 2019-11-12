package indexer.taxonomy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.ResourceBundle;

import static indexer.util.RowUtils.hasFieldName;

/**
 * @author lgonzales
 * @since 2019-10-08
 */
public class TaxonomyRDDReader {


    public static JavaPairRDD<String, TaxonomyEntry> load(JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        return (JavaPairRDD<String, TaxonomyEntry>) loadTaxonomyNodeRow(sparkContext, applicationConfig)
                .toJavaRDD()
                .mapToPair(new TaxonomyRowMapper());
    }

    public static JavaPairRDD<String, TaxonomyEntry> loadWithLineage(JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        JavaPairRDD<String, TaxonomyEntry> taxonomyNode = load(sparkContext, applicationConfig);

        JavaPairRDD<String, List<TaxonomyLineage>> taxonomyLineage = TaxonomyLineageReader
                .readTaxonomyLineage(sparkContext, applicationConfig);

        return (JavaPairRDD<String, TaxonomyEntry>) taxonomyNode.join(taxonomyLineage)
                .mapValues(new TaxonomyJoinMapper());
    }

    private static Dataset<Row> loadTaxonomyNodeRow(JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        long maxTaxId = getMaxTaxId(sparkContext, applicationConfig);
        int numberPartition = Integer.valueOf(applicationConfig.getString("database.taxonomy.partition"));
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();
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
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        Dataset<Row> max = spark.read()
                .format("jdbc")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("url", applicationConfig.getString("database.url"))
                .option("user", applicationConfig.getString("database.user.name"))
                .option("password", applicationConfig.getString("database.password"))
                .option("query", "SELECT MAX(TAX_ID) AS MAX_TAX_ID FROM TAXONOMY.V_PUBLIC_NODE")
                .load();

        Row result = max.head();
        return result.getDecimal(result.fieldIndex("MAX_TAX_ID")).intValue();
    }

    private static class TaxonomyRowMapper implements PairFunction<Row, String, TaxonomyEntry>, Serializable {

        private static final long serialVersionUID = -7723532417214033169L;

        @Override
        public Tuple2<String, TaxonomyEntry> call(Row rowValue) throws Exception {
            BigDecimal taxId = rowValue.getDecimal(rowValue.fieldIndex("TAX_ID"));
            TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder();
            builder.taxonId(taxId.longValue());

            if (hasFieldName("SPTR_COMMON", rowValue)) {
                builder.commonName(rowValue.getString(rowValue.fieldIndex("SPTR_COMMON")));
            } else if (hasFieldName("NCBI_COMMON", rowValue)) {
                builder.commonName(rowValue.getString(rowValue.fieldIndex("NCBI_COMMON")));
            }

            if (hasFieldName("SPTR_SCIENTIFIC", rowValue)) {
                builder.scientificName(rowValue.getString(rowValue.fieldIndex("SPTR_SCIENTIFIC")));
            } else if (hasFieldName("NCBI_SCIENTIFIC", rowValue)) {
                builder.scientificName(rowValue.getString(rowValue.fieldIndex("NCBI_SCIENTIFIC")));
            }

            if (hasFieldName("TAX_CODE", rowValue)) {
                builder.mnemonic(rowValue.getString(rowValue.fieldIndex("TAX_CODE")));
            }

            if (hasFieldName("PARENT_ID", rowValue)) {
                builder.parentId(rowValue.getDecimal(rowValue.fieldIndex("PARENT_ID")).longValue());
            }

            if (hasFieldName("RANK", rowValue)) {
                String rank = rowValue.getString(rowValue.fieldIndex("RANK"));
                builder.rank(TaxonomyRank.typeOf(rank));
            } else {
                builder.rank(TaxonomyRank.NO_RANK);
            }

            if (hasFieldName("SPTR_SYNONYM", rowValue)) {
                builder.addSynonyms(rowValue.getString(rowValue.fieldIndex("SPTR_SYNONYM")));
            }

            if (hasFieldName("HIDDEN", rowValue)) {
                builder.hidden(rowValue.getDecimal(rowValue.fieldIndex("HIDDEN")).intValue() == 1);
            }

            builder.active(true);

            return new Tuple2<String, TaxonomyEntry>(String.valueOf(taxId), builder.build());
        }

    }


    private static class TaxonomyJoinMapper implements Function<Tuple2<TaxonomyEntry, List<TaxonomyLineage>>, TaxonomyEntry>, Serializable {

        private static final long serialVersionUID = 7479649182382873120L;

        @Override
        public TaxonomyEntry call(Tuple2<TaxonomyEntry, List<TaxonomyLineage>> tuple) throws Exception {
            TaxonomyEntry entry = tuple._1;
            List<TaxonomyLineage> lineage = tuple._2;

            TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder().from(entry);
            builder.lineage(lineage);

            return builder.build();
        }
    }


    public static void main(String[] args) {
        ResourceBundle applicationConfig = ResourceBundle.getBundle("application");

        SparkConf conf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));
        System.out.println("******************* STARTING TAXONOMY LOAD *************************");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //System.out.println("MAX ID"+ TaxonomyRDDReader.getMaxTaxId(sc, applicationConfig));

        JavaPairRDD<String, TaxonomyEntry> taxonomyDataset = TaxonomyRDDReader.loadWithLineage(sc, applicationConfig);

        System.out.println("Taxonomy JavaPairRDD COUNT: " + taxonomyDataset.count());

        System.out.println("FINISHED TAXONOMY NODE");
        sc.close();
    }

}
