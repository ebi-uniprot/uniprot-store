package indexer.taxonomy;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;

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

    private final SparkConf sparkConf;
    private final ResourceBundle applicationConfig;

    public TaxonomyRDDReader(SparkConf sparkConf, ResourceBundle applicationConfig) {
        this.sparkConf = sparkConf;
        this.applicationConfig = applicationConfig;
    }

    public Dataset<Row> readTaxonomyNode() {
        return (Dataset<Row>) readTaxonomyNodeTable()
                .map(new TaxonomyRowMapper(), RowEncoder.apply(getTaxonomyRowSchema()));
    }

    public Dataset<Row> readTaxonomyNodeWithLineage() {
        Dataset<Row> taxonomyNode = readTaxonomyNodeTable();
        TaxonomyLineageReader taxonomyLineageReader = new TaxonomyLineageReader(sparkConf, applicationConfig);
        Dataset<Row> taxonomyLineage = taxonomyLineageReader.readTaxonomyLineage();
        Column taxId = taxonomyNode.col("TAX_ID");
        Column lineageTaxId = taxonomyLineage.col("TAX_ID");

        ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(getTaxonomyRowSchema());
        return (Dataset<Row>) taxonomyNode.join(taxonomyLineage, taxId.equalTo(lineageTaxId))
                .map(new TaxonomyRowMapper(), rowEncoder);
    }

    public static StructType getTaxonomyRowSchema() {
        StructType structType = new StructType();
        structType = structType.add("TAX_ID", DataTypes.createDecimalType(10, 0), false);
        structType = structType.add("TAXONOMY_ENTRY", DataTypes.BinaryType, false);
        return structType;
    }

    private Dataset<Row> readTaxonomyNodeTable() {
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        return spark.read()
                .format("jdbc")
                .option("url", applicationConfig.getString("database.url"))
                .option("user", applicationConfig.getString("database.user.name"))
                .option("password", applicationConfig.getString("database.password"))
                .option("dbtable", "taxonomy.v_public_node")
                .load();
    }

    private static class TaxonomyRowMapper implements MapFunction<Row, Row>, Serializable {

        private static final long serialVersionUID = -7723532417214033169L;

        @Override
        public Row call(Row rowValue) throws Exception {
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

            if (hasFieldName("TAX_LINEAGE", rowValue)) {
                List<Object> taxonomyLineageBytes = (List<Object>) rowValue.getList(rowValue.fieldIndex("TAX_LINEAGE"));
                taxonomyLineageBytes.forEach(lineageItem -> {
                    builder.addLineage(SerializationUtils.deserialize((byte[]) lineageItem));
                });
            }

            builder.active(true);

            final Object[] rowColumns = {taxId, SerializationUtils.serialize(builder.build())};
            return new GenericRowWithSchema(rowColumns, getTaxonomyRowSchema());
        }

    }

    public static void main(String[] args) {
        ResourceBundle applicationConfig = ResourceBundle.getBundle("application");

        SparkConf conf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));
        System.out.println("******************* STARTING TAXONOMY LOAD *************************");
        JavaSparkContext sc = new JavaSparkContext(conf);

        TaxonomyRDDReader taxonomyNodeReader = new TaxonomyRDDReader(conf, applicationConfig);
        Dataset<Row> taxonomyDataset = taxonomyNodeReader.readTaxonomyNodeWithLineage();

        taxonomyDataset.printSchema();

        taxonomyDataset.show();

        sc.close();
    }

}
