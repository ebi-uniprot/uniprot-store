package org.uniprot.store.spark.indexer.chebi;

import static org.apache.spark.sql.functions.*;
import static org.uniprot.store.indexer.common.utils.Constants.*;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryMapper.CHEMROF_INCHI_KEY_STRING;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryRelatedFieldsRowMapper.ABOUT_SUBJECT;
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
import org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryRowAggregator;
import org.uniprot.store.spark.indexer.common.JobParameter;

import com.typesafe.config.Config;

import scala.collection.JavaConverters;

public class ChebiOwlReader {

    private static final String OBO_IAO_0000115 = "obo:IAO_0000115";
    private static final String OBO_IN_OWL_ID = "oboInOwl:id";
    private static final String SUBJECT = "subject";
    private static final String OBJECT = "object";
    private static final String CHEBI_FILE_PATH = "chebi.file.path";
    private static final String COM_DATABRICKS_SPARK_XML = "com.databricks.spark.xml";
    private static final String ROW_TAG = "rowTag";
    private static final String RDF_DESCRIPTION = "rdf:Description";
    private static final String SUB_CLASS_OF = "subClassOf";
    private static final String ALIAS_A = "a";
    private static final String ALIAS_B = "b";
    private static final String A_SUBJECT = "a.subject";
    private static final String B_SUBJECT = "b.subject";
    private static final String B_OBJECT = "b.object";
    private static final String A_CHEBI_STRUCTURED_NAME = "a.chebiStructuredName";
    private static final String A_SUB_CLASS_OF = "a.subClassOf";
    private static final String A_ABOUT_SUBJECT = "a.about_subject";
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
                        .add(NAME, DataTypes.StringType, true)
                        .add(
                                CHEBI_RDF_TYPE_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(
                                                        CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                                        DataTypes.StringType,
                                                        true)),
                                true)
                        .add(
                                CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(
                                                        CHEBI_RDF_NODE_ID_ATTRIBBUTE,
                                                        DataTypes.StringType,
                                                        true)),
                                true)
                        .add(CHEMROF_INCHI_KEY_STRING, DataTypes.StringType, true)
                        .add(OBO_IAO_0000115, DataTypes.StringType, true)
                        .add(OBO_IN_OWL_ID, DataTypes.StringType, true)
                        .add(
                                CHEBI_RDFS_SUBCLASS_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(
                                                        CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                                        DataTypes.StringType,
                                                        true)
                                                .add(
                                                        CHEBI_RDF_NODE_ID_ATTRIBBUTE,
                                                        DataTypes.StringType,
                                                        true)),
                                true)
                        .add(
                                CHEBI_RDFS_LABEL_ATTRIBUTE,
                                DataTypes.createArrayType(DataTypes.StringType),
                                true)
                        .add(
                                CHEBI_OWL_PROPERTY_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(
                                                        CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                                        DataTypes.StringType,
                                                        true)),
                                true)
                        .add(
                                CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE,
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add(
                                                        CHEBI_RDF_RESOURCE_ATTRIBUTE,
                                                        DataTypes.StringType,
                                                        true)),
                                true);
        return schema;
    }

    private StructType getProcessedSchema() {
        StructType processedSchema =
                new StructType()
                        .add(SUBJECT, DataTypes.StringType)
                        .add(
                                OBJECT,
                                DataTypes.createMapType(
                                        DataTypes.StringType,
                                        DataTypes.createArrayType(DataTypes.StringType)));
        return processedSchema;
    }

    private StructType getExplodedSchema() {
        StructType explodedSchema =
                new StructType()
                        .add(ABOUT_SUBJECT, DataTypes.StringType)
                        .add(CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE, DataTypes.StringType)
                        .add(CHEBI_RDFS_SUBCLASS_ATTRIBUTE, DataTypes.StringType);
        return explodedSchema;
    }

    private JavaRDD<Row> readChebiFile() {
        Config config = jobParameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString(CHEBI_FILE_PATH);
        Dataset<Row> rdfDescriptions =
                this.spark
                        .read()
                        .format(COM_DATABRICKS_SPARK_XML)
                        .option(ROW_TAG, RDF_DESCRIPTION)
                        .schema(getSchema())
                        .load(filePath);
        return rdfDescriptions.toJavaRDD();
    }

    public JavaPairRDD<Long, ChebiEntry> load() {
        StructType processedSchema = getProcessedSchema();
        StructType explodedSchema = getExplodedSchema();
        JavaRDD<Row> rdfDescriptionsRDD = readChebiFile();
        JavaRDD<Row> processedAboutRDFDescriptionsRDD =
                getAboutJavaRDDFromDescription(rdfDescriptionsRDD);
        JavaRDD<Row> processedNodeIdRDFDescriptionsRDD =
                getNodeIdJavaRDDFromDescription(rdfDescriptionsRDD);
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
                        .groupBy(ABOUT_SUBJECT)
                        .agg(
                                collect_set(CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE)
                                        .alias(CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE),
                                collect_set(CHEBI_RDFS_SUBCLASS_ATTRIBUTE).alias(SUB_CLASS_OF));
        JavaRDD<Row> joinedNodeRDD =
                joinAndExtractLabelAndClassRelatedNodesFromNodeIdDF(
                        processedNodeIdDF, groupedExplodedAboutDF);
        Dataset<Row> joinedNodeDF = spark.createDataFrame(joinedNodeRDD, processedSchema);
        Dataset<Row> finalMergedDF =
                processedAboutDF
                        .as(ALIAS_A)
                        .join(
                                joinedNodeDF.as(ALIAS_B),
                                col(A_SUBJECT).equalTo(col(B_SUBJECT)),
                                "inner");
        finalMergedDF =
                finalMergedDF.selectExpr(A_SUBJECT, "map_concat(a.object, b.object) as object");
        JavaPairRDD<Long, ChebiEntry> chebiEntryPairRDD =
                finalMergedDF.toJavaRDD().mapToPair(new ChebiEntryMapper());
        return chebiEntryPairRDD;
    }

    private static JavaRDD<Row> getAboutJavaRDDFromDescription(JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedAboutRDFDescriptionsRDD =
                rdfDescriptionsRDD.map(new ChebiEntryRowMapper()).filter(Objects::nonNull);
        return processedAboutRDFDescriptionsRDD;
    }

    private static JavaRDD<Row> getNodeIdJavaRDDFromDescription(JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedNodeIdRDFDescriptionsRDD =
                rdfDescriptionsRDD.map(new ChebiNodeEntryRowMapper()).filter(Objects::nonNull);
        return processedNodeIdRDFDescriptionsRDD;
    }

    private static Dataset<Row> getLabelAndClassColumnsFromAboutRDD(
            StructType explodedSchema, Dataset<Row> processedAboutDF) {
        Dataset<Row> explodedAboutDF =
                processedAboutDF
                        .selectExpr("subject AS about_subject", OBJECT)
                        .flatMap(
                                new ChebiEntryRelatedFieldsRowMapper(),
                                RowEncoder.apply(explodedSchema));
        return explodedAboutDF;
    }

    private static JavaRDD<Row> joinAndExtractLabelAndClassRelatedNodesFromNodeIdDF(
            Dataset<Row> processedNodeIdDF, Dataset<Row> groupedAboutDF) {
        JavaRDD<Row> joinedNodeRDD =
                groupedAboutDF
                        .alias(ALIAS_A)
                        .join(
                                processedNodeIdDF.alias(ALIAS_B),
                                array_contains(col(A_CHEBI_STRUCTURED_NAME), col(B_SUBJECT))
                                        .or(array_contains(col(A_SUB_CLASS_OF), col(B_SUBJECT))),
                                "inner")
                        .select(col(A_ABOUT_SUBJECT), col(B_SUBJECT), col(B_OBJECT))
                        .toJavaRDD()
                        .flatMapToPair(new ChebiNodeEntryRelatedFieldsRowMapper())
                        .aggregateByKey(
                                null, new ChebiEntryRowAggregator(), new ChebiEntryRowAggregator())
                        .map(
                                row ->
                                        RowFactory.create(
                                                row._1, JavaConverters.mapAsScalaMap(row._2)));
        return joinedNodeRDD;
    }
}
