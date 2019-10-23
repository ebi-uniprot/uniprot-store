package indexer.taxonomy;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.core.util.Utils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-11
 */
public class TaxonomyLineageReader {

    private static final String SELECT_TAXONOMY_LINEAGE_SQL = "SELECT" +
            "   SYS_CONNECT_BY_PATH(TAX_ID, '|') AS lineage_id," +
            "   SYS_CONNECT_BY_PATH(SPTR_SCIENTIFIC, '|') AS lineage_name," +
            "   SYS_CONNECT_BY_PATH(RANK, '|') AS lineage_rank," +
            "   SYS_CONNECT_BY_PATH(HIDDEN, '|') AS lineage_hidden" +
            " FROM taxonomy.V_PUBLIC_NODE" +
            " WHERE TAX_ID = 1" +
            " CONNECT BY PRIOR PARENT_ID = TAX_ID";


    private final SparkConf sparkConf;
    private final ResourceBundle applicationConfig;

    public TaxonomyLineageReader(SparkConf sparkConf, ResourceBundle applicationConfig) {
        this.sparkConf = sparkConf;
        this.applicationConfig = applicationConfig;
    }

    public Dataset<Row> readTaxonomyLineage() {
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        Dataset<Row> tableDataset = spark.read()
                .format("jdbc")
                .option("url", applicationConfig.getString("database.url"))
                .option("user", applicationConfig.getString("database.user.name"))
                .option("password", applicationConfig.getString("database.password"))
                .option("query", SELECT_TAXONOMY_LINEAGE_SQL)
                .load();
        return mapToLineageRow(tableDataset);
    }

    private Dataset<Row> mapToLineageRow(Dataset<Row> tableDataset) {
        ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(getLineageRowSchema());
        return (Dataset<Row>) tableDataset.map(new LineageRowMapper(), rowEncoder);
    }

    public static StructType getLineageRowSchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.createDecimalType(10, 0), false);
        structType = structType.add("TAX_LINEAGE", DataTypes.createArrayType(DataTypes.BinaryType), false);
        return structType;
    }

    private static class LineageRowMapper implements MapFunction<Row, Row>, Serializable {

        private static final long serialVersionUID = -7723532417214033169L;

        @Override
        public Row call(Row rowValue) throws Exception {
            String lineageId = rowValue.getString(rowValue.fieldIndex("LINEAGE_ID"));
            String lineageName = rowValue.getString(rowValue.fieldIndex("LINEAGE_NAME"));
            String lineageRank = rowValue.getString(rowValue.fieldIndex("LINEAGE_RANK"));
            String lineageHidden = rowValue.getString(rowValue.fieldIndex("LINEAGE_HIDDEN"));

            String[] lineageIdArray = lineageId.substring(1).split("\\|");
            String[] lineageRankArray = lineageRank.substring(1).split("\\|");
            String[] lineageHiddenArray = lineageHidden.substring(1).split("\\|");
            String[] lineageNameArray = lineageName.substring(1).split("\\|");

            String taxId = lineageIdArray[0];
            byte[][] lineageList = new byte[lineageIdArray.length - 1][];
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
                lineageList[i - 1] = SerializationUtils.serialize(builder.build());
            }

            final Object[] rowColumns = {new BigDecimal(taxId), lineageList};
            return new GenericRowWithSchema(rowColumns, getLineageRowSchema());
        }

    }


    public static void main(String[] args) {
        ResourceBundle applicationConfig = ResourceBundle.getBundle("application");

        SparkConf conf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));
        System.out.println("******************* STARTING TAXONOMY LOAD *************************");
        JavaSparkContext sc = new JavaSparkContext(conf);

        TaxonomyLineageReader taxonomyLineageReader = new TaxonomyLineageReader(conf, applicationConfig);
        Dataset<Row> taxonomyLineage = taxonomyLineageReader.readTaxonomyLineage();
        taxonomyLineage.printSchema();

        Row[] rowList = (Row[]) taxonomyLineage.head(10);
        Row row = rowList[5];
        List<Object> lineage = row.getList(row.fieldIndex("TAX_LINEAGE"));

        System.out.println(lineage);


        sc.close();
    }

}
