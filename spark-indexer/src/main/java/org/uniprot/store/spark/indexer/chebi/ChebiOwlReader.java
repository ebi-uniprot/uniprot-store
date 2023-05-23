package org.uniprot.store.spark.indexer.chebi;

import static org.apache.spark.sql.functions.*;
import static org.uniprot.store.indexer.common.utils.Constants.*;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.chebi.mapper.*;
import org.uniprot.store.spark.indexer.common.JobParameter;

import scala.collection.JavaConverters;
import org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryRowAggregator;
public class ChebiOwlReader {

    private final SparkSession spark;
    private final JobParameter jobParameter;

    public ChebiOwlReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        JavaSparkContext jsc = this.jobParameter.getSparkContext();
        this.spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
    }

    public static StructType getSchema() {
        StructType schema =
                new StructType()
                        .add(CHEBI_RDF_ABOUT_ATTRIBUTE, DataTypes.StringType, true)
                        .add(CHEBI_RDF_NODE_ID_ATTRIBBUTE, DataTypes.StringType, true)
                        .add("name", DataTypes.StringType, true)
                        .add(
                                CHEBI_RDF_TYPE_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(CHEBI_RDF_RESOURCE_ATTRIBUTE, DataTypes.StringType, true)),
                                true)
                        .add(
                                CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(CHEBI_RDF_NODE_ID_ATTRIBBUTE, DataTypes.StringType, true)),
                                true)
                        .add("chebislash:inchikey", DataTypes.StringType, true)
                        .add("obo:IAO_0000115", DataTypes.StringType, true)
                        .add("oboInOwl:hasId", DataTypes.StringType, true)
                        .add(
                                CHEBI_RDFS_SUBCLASS_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(CHEBI_RDF_RESOURCE_ATTRIBUTE, DataTypes.StringType, true)
                                                .add(CHEBI_RDF_NODE_ID_ATTRIBBUTE, DataTypes.StringType, true)),
                                true)
                        .add(CHEBI_RDFS_LABEL_ATTRIBUTE, DataTypes.createArrayType(DataTypes.StringType), true)
                        .add(
                                CHEBI_OWL_PROPERTY_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(CHEBI_RDF_RESOURCE_ATTRIBUTE, DataTypes.StringType, true)),
                                true)
                        .add(
                                CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(CHEBI_RDF_RESOURCE_ATTRIBUTE, DataTypes.StringType, true)),
                                true);
        return schema;
    }

    private StructType getProcessedSchema() {
        StructType processedSchema =
                new StructType()
                        .add("subject", DataTypes.StringType)
                        .add(
                                "object",
                                DataTypes.createMapType(
                                        DataTypes.StringType,
                                        DataTypes.createArrayType(DataTypes.StringType)));
        return processedSchema;
    }

    private StructType getExplodedSchema() {
        StructType explodedSchema =
                new StructType()
                        .add("about_subject", DataTypes.StringType)
                        .add(CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE, DataTypes.StringType)
                        .add(CHEBI_RDFS_SUBCLASS_ATTRIBUTE, DataTypes.StringType);
        return explodedSchema;
    }

    private JavaRDD<Row> readChebiFile() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("chebi.file.path");
        Dataset<Row> rdfDescriptions =
                this.spark
                        .read()
                        .format("com.databricks.spark.xml")
                        .option("rowTag", "rdf:Description")
                        .schema(getSchema())
                        .load(filePath);
        return rdfDescriptions.toJavaRDD();
    }

    public JavaPairRDD<Long, ChebiEntry> load() {
        StructType schema = getSchema();
        StructType processedSchema = getProcessedSchema();
        StructType explodedSchema = getExplodedSchema();
        JavaRDD<Row> rdfDescriptionsRDD = readChebiFile();
        JavaRDD<Row> processedAboutRDFDescriptionsRDD =
                getAboutJavaRDDFromDescription(schema, rdfDescriptionsRDD);
        JavaRDD<Row> processedNodeIdRDFDescriptionsRDD =
                getNodeIdJavaRDDFromDescription(schema, rdfDescriptionsRDD);
        Dataset<Row> processedAboutDF =
                spark.createDataFrame(processedAboutRDFDescriptionsRDD, processedSchema)
                        .filter(Objects::nonNull);
        Dataset<Row> processedNodeIdDF =
                spark.createDataFrame(processedNodeIdRDFDescriptionsRDD, processedSchema)
                        .filter(Objects::nonNull);
        Dataset<Row> explodedAboutDF =
                getLabelAndClassColumnsFromAboutRDD(explodedSchema, processedAboutDF);
        Dataset<Row> groupedExplodedAboutDF =
                explodedAboutDF
                        .groupBy("about_subject")
                        .agg(
                                collect_set(CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE).alias(CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE),
                                collect_set(CHEBI_RDFS_SUBCLASS_ATTRIBUTE).alias("subClassOf"));
        JavaRDD<Row> joinedNodeRDD =
                joinAndExtractLabelAndClassRelatedNodesFromNodeIdDF(
                        processedNodeIdDF, groupedExplodedAboutDF);
        Dataset<Row> joinedNodeDF = spark.createDataFrame(joinedNodeRDD, processedSchema);
        Dataset<Row> finalMergedDF =
                processedAboutDF
                        .as("a")
                        .join(
                                joinedNodeDF.as("b"),
                                col("a.subject").equalTo(col("b.subject")),
                                "inner");
        finalMergedDF =
                finalMergedDF.selectExpr("a.subject", "map_concat(a.object, b.object) as object");
        JavaPairRDD<Long, ChebiEntry> chebiEntryPairRDD =
                finalMergedDF.toJavaRDD().mapToPair(new ChebiEntryMapper());
        return chebiEntryPairRDD;
    }

    private static JavaRDD<Row> getAboutJavaRDDFromDescription(
            StructType schema, JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedAboutRDFDescriptionsRDD =
                rdfDescriptionsRDD.flatMap(new ChebiEntryRowMapper()).filter(Objects::nonNull);
        return processedAboutRDFDescriptionsRDD;
    }

    private static JavaRDD<Row> getNodeIdJavaRDDFromDescription(
            StructType schema, JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedNodeIdRDFDescriptionsRDD =
                rdfDescriptionsRDD.flatMap(new ChebiNodeEntryRowMapper()).filter(Objects::nonNull);
        return processedNodeIdRDFDescriptionsRDD;
    }

    private static Dataset<Row> getLabelAndClassColumnsFromAboutRDD(
            StructType explodedSchema, Dataset<Row> processedAboutDF) {
        Dataset<Row> explodedAboutDF =
                processedAboutDF
                        .selectExpr("subject AS about_subject", "object")
                        .flatMap(
                                new ChebiEntryRelatedFieldsRowMapper(),
                                RowEncoder.apply(explodedSchema));
        return explodedAboutDF;
    }

    private static JavaRDD<Row> joinAndExtractLabelAndClassRelatedNodesFromNodeIdDF(
            Dataset<Row> processedNodeIdDF, Dataset<Row> groupedAboutDF) {
        JavaRDD<Row> joinedNodeRDD =
                groupedAboutDF
                        .alias("a")
                        .join(
                                processedNodeIdDF.alias("b"),
                                array_contains(col("a.chebiStructuredName"), col("b.subject"))
                                        .or(array_contains(col("a.subClassOf"), col("b.subject"))),
                                "inner")
                        .select(col("a.about_subject"), col("b.subject"), col("b.object"))
                        .toJavaRDD()
                        .flatMapToPair(new ChebiNodeEntryRelatedFieldsRowMapper())
                        .aggregateByKey(null, new ChebiEntryRowAggregator(), new ChebiEntryRowAggregator())
                        .map(
                                row ->
                                        RowFactory.create(
                                                row._1, JavaConverters.mapAsScalaMap(row._2)));
        return joinedNodeRDD;
    }
}
