package indexer.taxonomy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.core.util.Utils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-11
 */
public class TaxonomyLineageReader {

    private static final String SELECT_TAXONOMY_LINEAGE_SQL = "SELECT " +
            "   CONNECT_BY_ROOT TAX_ID AS TAX_ID," +
            "   SYS_CONNECT_BY_PATH(TAX_ID, '|') AS lineage_id," +
            "   SYS_CONNECT_BY_PATH(COALESCE(SPTR_SCIENTIFIC,NCBI_SCIENTIFIC), '|') AS lineage_name," +
            "   SYS_CONNECT_BY_PATH(COALESCE(SPTR_COMMON, NCBI_COMMON), '|') AS lineage_common," +
            "   SYS_CONNECT_BY_PATH(SPTR_SYNONYM, '|') AS lineage_synonym," +
            "   SYS_CONNECT_BY_PATH(RANK, '|') AS lineage_rank," +
            "   SYS_CONNECT_BY_PATH(HIDDEN, '|') AS lineage_hidden" +
            " FROM taxonomy.V_PUBLIC_NODE" +
            " WHERE TAX_ID = 1" +
            " START WITH TAX_ID >= {start} AND TAX_ID <= {end} " +
            " CONNECT BY PRIOR PARENT_ID = TAX_ID";

    public static JavaPairRDD<String, List<TaxonomyLineage>> readTaxonomyLineage(JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        int maxTaxId = TaxonomyRDDReader.getMaxTaxId(sparkContext, applicationConfig);
        System.out.println("MAx tax id: " + maxTaxId);

        SparkSession spark = SparkSession
                .builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        int numberPartition = Integer.valueOf(applicationConfig.getString("database.lineage.partition"));
        int[][] ranges = getRanges(maxTaxId, numberPartition);
        List<JavaPairRDD<String, List<TaxonomyLineage>>> datasets = new ArrayList<>();
        for (int[] range : ranges) {
            String sql = SELECT_TAXONOMY_LINEAGE_SQL
                    .replace("{start}", "" + range[0])
                    .replace("{end}", "" + range[1]);

            System.out.println("SQL: " + sql);

            Dataset<Row> tableDataset = spark.read()
                    .format("jdbc")
                    .option("driver", "oracle.jdbc.driver.OracleDriver")
                    .option("url", applicationConfig.getString("database.url"))
                    .option("user", applicationConfig.getString("database.user.name"))
                    .option("password", applicationConfig.getString("database.password"))
                    .option("query", sql)
                    .load();
            datasets.add(mapToLineage(tableDataset));
        }
        return (JavaPairRDD<String, List<TaxonomyLineage>>) datasets.stream().reduce(JavaPairRDD::union)
                .orElseThrow(() -> new IllegalStateException("Unable to union lineage datasets"));
    }

    private static int[][] getRanges(int maxId, int numPartition) {
        int rangeSize = Math.floorDiv(maxId, numPartition);
        int start = 1;
        int[][] range = new int[numPartition][2];
        for (int i = 0; i < numPartition; i++) {
            range[i] = new int[]{start, start + rangeSize};
            start = start + rangeSize + 1;
        }
        return range;
    }

    private static JavaPairRDD<String, List<TaxonomyLineage>> mapToLineage(Dataset<Row> tableDataset) {
        return (JavaPairRDD<String, List<TaxonomyLineage>>) tableDataset.toJavaRDD().mapToPair(new LineageRowMapper());
    }

    private static class LineageRowMapper implements PairFunction<Row, String, List<TaxonomyLineage>>, Serializable {

        private static final long serialVersionUID = -7723532417214033169L;

        @Override
        public Tuple2<String, List<TaxonomyLineage>> call(Row rowValue) throws Exception {
            String lineageId = rowValue.getString(rowValue.fieldIndex("LINEAGE_ID"));
            String lineageName = rowValue.getString(rowValue.fieldIndex("LINEAGE_NAME"));
            String lineageRank = rowValue.getString(rowValue.fieldIndex("LINEAGE_RANK"));
            String lineageHidden = rowValue.getString(rowValue.fieldIndex("LINEAGE_HIDDEN"));

            String[] lineageIdArray = lineageId.substring(1).split("\\|");
            String[] lineageRankArray = lineageRank.substring(1).split("\\|");
            String[] lineageHiddenArray = lineageHidden.substring(1).split("\\|");
            String[] lineageNameArray = lineageName.substring(1).split("\\|");

            String taxId = lineageIdArray[0];
            List<TaxonomyLineage> lineageList = new ArrayList<>();
            for (int i = 1; i < lineageIdArray.length - 1; i++) {
                TaxonomyLineageBuilder builder = new TaxonomyLineageBuilder();
                builder.taxonId(Long.parseLong(lineageIdArray[i]));
                builder.scientificName(lineageNameArray[i]);
                builder.hidden(lineageHiddenArray[i].equals("1"));
                if (Utils.notEmpty(lineageRankArray[i])) {
                    try {
                        builder.rank(TaxonomyRank.valueOf(lineageRankArray[i].toUpperCase()));
                    } catch (IllegalArgumentException iae) {
                        builder.rank(TaxonomyRank.NO_RANK);
                    }
                }
                lineageList.add(builder.build());
            }

            return new Tuple2<>(taxId, lineageList);
        }

    }

    public static void main(String[] args) {
        ResourceBundle applicationConfig = ResourceBundle.getBundle("application");

        SparkConf conf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));
        System.out.println("******************* STARTING TAXONOMY LINEAGE LOAD *************************");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, List<TaxonomyLineage>> taxonomyLineage = TaxonomyLineageReader.readTaxonomyLineage(sc, applicationConfig);

        System.out.println("LINEAGE COUNT: " + taxonomyLineage.count());

        Tuple2<String, List<TaxonomyLineage>> rowList = (Tuple2<String, List<TaxonomyLineage>>) taxonomyLineage.first();
        System.out.println(rowList._2);


        sc.close();
    }

}
