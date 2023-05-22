package org.uniprot.store.spark.indexer.chebi;

import static org.apache.spark.sql.functions.*;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.chebi.mapper.*;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ChebiOwlReader {

    private final SparkSession spark;
    private final JobParameter jobParameter;

    public ChebiOwlReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        JavaSparkContext jsc = this.jobParameter.getSparkContext();
        this.spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
    }

    public static final Set<String> unwantedAboutValues =
            new HashSet<>(
                    Arrays.asList(
                            "http://purl.obolibrary.org/obo/BFO_0000051",
                            "http://purl.obolibrary.org/obo/BFO_0000050",
                            "http://purl.obolibrary.org/obo/RO_0000087",
                            "http://purl.obolibrary.org/obo/chebi#has_parent_hydride",
                            "http://purl.obolibrary.org/obo/chebi#has_functional_parent",
                            "http://purl.obolibrary.org/obo/chebi#is_conjugate_acid_of",
                            "http://purl.obolibrary.org/obo/chebi#is_conjugate_base_of",
                            "http://purl.obolibrary.org/obo/chebi#is_enantiomer_of",
                            "http://purl.obolibrary.org/obo/chebi#is_substituent_group_from",
                            "http://purl.obolibrary.org/obo/chebi#is_tautomer_of",
                            "http://purl.obolibrary.org/obo/IAO_0000115",
                            "http://purl.obolibrary.org/obo/IAO_0000231",
                            "http://purl.obolibrary.org/obo/IAO_0100001",
                            "http://purl.obolibrary.org/obo/chebi/charge",
                            "http://purl.obolibrary.org/obo/chebi/formula",
                            "http://purl.obolibrary.org/obo/chebi/inchi",
                            "http://purl.obolibrary.org/obo/chebi/inchikey",
                            "http://purl.obolibrary.org/obo/chebi/mass",
                            "http://purl.obolibrary.org/obo/chebi/monoisotopicmass",
                            "http://purl.obolibrary.org/obo/chebi/smiles",
                            "http://www.geneontology.org/formats/oboInOwl#hasDbXref",
                            "http://www.geneontology.org/formats/oboInOwl#hasId"));

    public static StructType getSchema() {
        StructType schema =
                new StructType()
                        .add("_rdf:about", DataTypes.StringType, true)
                        .add("_rdf:nodeID", DataTypes.StringType, true)
                        .add("name", DataTypes.StringType, true)
                        .add(
                                "rdf:type",
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add("_rdf:resource", DataTypes.StringType, true)),
                                true)
                        .add(
                                "chebiStructuredName",
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add("_rdf:nodeID", DataTypes.StringType, true)),
                                true)
                        .add("chebislash:inchikey", DataTypes.StringType, true)
                        .add("obo:IAO_0000115", DataTypes.StringType, true)
                        .add("oboInOwl:hasId", DataTypes.StringType, true)
                        .add(
                                "rdfs:subClassOf",
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add("_rdf:resource", DataTypes.StringType, true)
                                                .add("_rdf:nodeID", DataTypes.StringType, true)),
                                true)
                        .add("rdfs:label", DataTypes.createArrayType(DataTypes.StringType), true)
                        .add(
                                "owl:onProperty",
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add("_rdf:resource", DataTypes.StringType, true)),
                                true)
                        .add(
                                "owl:someValuesFrom",
                                DataTypes.createArrayType(
                                        new StructType()
                                                .add("_rdf:resource", DataTypes.StringType, true)),
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
                        .add("chebiStructuredName", DataTypes.StringType)
                        .add("rdfs:subClassOf", DataTypes.StringType);
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
                                collect_set("chebiStructuredName").alias("chebiStructuredName"),
                                collect_set("rdfs:subClassOf").alias("subClassOf"));
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

    protected static JavaRDD<Row> getAboutJavaRDDFromDescription(
            StructType schema, JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedAboutRDFDescriptionsRDD =
                rdfDescriptionsRDD.flatMap(new ChebiEntryRowMapper()).filter(Objects::nonNull);
        return processedAboutRDFDescriptionsRDD;
    }

    protected static JavaRDD<Row> getNodeIdJavaRDDFromDescription(
            StructType schema, JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedNodeIdRDFDescriptionsRDD =
                rdfDescriptionsRDD.flatMap(new ChebiNodeEntryRowMapper()).filter(Objects::nonNull);
        return processedNodeIdRDFDescriptionsRDD;
    }

    protected static Dataset<Row> getLabelAndClassColumnsFromAboutRDD(
            StructType explodedSchema, Dataset<Row> processedAboutDF) {
        Dataset<Row> explodedAboutDF =
                processedAboutDF
                        .selectExpr("subject AS about_subject", "object")
                        .flatMap(
                                new ChebiEntryRelatedFieldsRowMapper(),
                                RowEncoder.apply(explodedSchema));
        return explodedAboutDF;
    }

    protected static JavaRDD<Row> joinAndExtractLabelAndClassRelatedNodesFromNodeIdDF(
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
                        .aggregateByKey(null, aggregate(), aggregate())
                        .map(
                                row ->
                                        RowFactory.create(
                                                row._1, JavaConverters.mapAsScalaMap(row._2)));
        return joinedNodeRDD;
    }

    private static Function2<
                    Map<String, Seq<String>>, Map<String, Seq<String>>, Map<String, Seq<String>>>
            aggregate() {
        return (map1, map2) -> {
            if (SparkUtils.isThereAnyNullEntry(map1, map2)) {
                map1 = SparkUtils.getNotNullEntry(map1, map2);
            } else {
                for (Map.Entry<String, Seq<String>> entry : map2.entrySet()) {
                    String key = entry.getKey();
                    Seq<String> value = entry.getValue();
                    if (map1.containsKey(key)) {
                        List<String> combinedValue =
                                new ArrayList<>(JavaConverters.seqAsJavaList(map1.get(key)));
                        combinedValue.addAll(JavaConverters.seqAsJavaList(value));
                        map1.put(key, JavaConverters.asScalaBuffer(combinedValue).toSeq());
                    } else {
                        map1.put(key, value);
                    }
                }
            }
            return map1;
        };
    }
}
