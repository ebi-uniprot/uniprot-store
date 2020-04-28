package org.uniprot.store.spark.indexer.taxonomy;

import java.util.List;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.taxonomy.TaxonomyLineage;

/**
 * This class is responsible to read values from Lineage into an a JavaPairRDD{key=taxId, value=List
 * of TaxonomyLineage}
 *
 * <p>In order to improve the performance there is a logic to partition the query based on the
 * number of partitions (database.lineage.partition).
 *
 * <p>It basically get the maxTaxId and divide by the number of partition, to create a range for
 * each partition
 *
 * <p>After that it executes one query for each partition based on the partition range.
 *
 * @author lgonzales
 * @since 2019-10-11
 */
@Slf4j
public class TaxonomyLineageReader {

    private TaxonomyLineageReader() {}

    private static final String SELECT_TAXONOMY_LINEAGE_SQL =
            "SELECT "
                    + "   CONNECT_BY_ROOT TAX_ID AS TAX_ID,"
                    + "   SYS_CONNECT_BY_PATH(TAX_ID, '|') AS lineage_id,"
                    + "   SYS_CONNECT_BY_PATH(COALESCE(SPTR_SCIENTIFIC,NCBI_SCIENTIFIC), '|') AS lineage_name,"
                    + "   SYS_CONNECT_BY_PATH(COALESCE(SPTR_COMMON, NCBI_COMMON, ' '), '|') AS lineage_common,"
                    + "   SYS_CONNECT_BY_PATH(COALESCE(SPTR_SYNONYM, ' '), '|') AS lineage_synonym,"
                    + "   SYS_CONNECT_BY_PATH(RANK, '|') AS lineage_rank,"
                    + "   SYS_CONNECT_BY_PATH(HIDDEN, '|') AS lineage_hidden"
                    + " FROM taxonomy.V_PUBLIC_NODE"
                    + " WHERE TAX_ID = 1"
                    + " START WITH TAX_ID >= {start} AND TAX_ID <= {end} "
                    + " CONNECT BY PRIOR PARENT_ID = TAX_ID";

    /** @return JavaPairRDD{key=taxId, value=List of TaxonomyLineage} */
    public static JavaPairRDD<String, List<TaxonomyLineage>> load(
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig,
            boolean includeOrganism) {
        int maxTaxId = TaxonomyRDDReader.getMaxTaxId(sparkContext, applicationConfig);
        log.info("Max tax id: " + maxTaxId);

        SparkSession spark = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();

        int numberPartition =
                Integer.parseInt(applicationConfig.getString("database.lineage.partition"));
        int[][] ranges = getRanges(maxTaxId, numberPartition);
        JavaPairRDD<String, List<TaxonomyLineage>> result = null;
        for (int[] range : ranges) {
            String sql =
                    SELECT_TAXONOMY_LINEAGE_SQL
                            .replace("{start}", "" + range[0])
                            .replace("{end}", "" + range[1]);

            log.info("SQL: " + sql);

            Dataset<Row> tableDataset =
                    spark.read()
                            .format("jdbc")
                            .option("driver", "oracle.jdbc.driver.OracleDriver")
                            .option("url", applicationConfig.getString("database.url"))
                            .option("user", applicationConfig.getString("database.user.name"))
                            .option("password", applicationConfig.getString("database.password"))
                            .option("query", sql)
                            .load();
            if (result == null) {
                result = mapToLineage(tableDataset, includeOrganism);
            } else {
                result = result.union(mapToLineage(tableDataset, includeOrganism));
            }
        }
        return result;
    }

    static int[][] getRanges(int maxId, int numPartition) {
        int rangeSize = (int) Math.ceil((double) maxId / numPartition);
        int start = 0;
        int[][] range = new int[numPartition][2];
        for (int i = 0; i < numPartition; i++) {
            range[i] = new int[] {start + 1, start + rangeSize};
            start = start + rangeSize;
        }
        if (range[numPartition - 1][1] > maxId) {
            range[numPartition - 1][1] = maxId;
        }
        return range;
    }

    private static JavaPairRDD<String, List<TaxonomyLineage>> mapToLineage(
            Dataset<Row> tableDataset, boolean includeOrganism) {
        return (JavaPairRDD<String, List<TaxonomyLineage>>)
                tableDataset.toJavaRDD().mapToPair(new TaxonomyLineageRowMapper(includeOrganism));
    }
}
