package org.uniprot.store.spark.indexer.chebi;

import static org.apache.spark.sql.functions.*;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryMapper;
import org.uniprot.store.spark.indexer.common.JobParameter;

import scala.Tuple2;
import scala.collection.AbstractSeq;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ChebiOwlReader {

    private static final String SPARK_MASTER = "spark.master";

    private static final String SPARK_JARS = "spark.jars.packages";
    SparkSession spark;

    private final JobParameter jobParameter;

    public ChebiOwlReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        createSparkSession();
    }

    public void createSparkSession() {
        String applicationName =
                jobParameter.getApplicationConfig().getString("spark.application.name");
        String sparkMaster = jobParameter.getApplicationConfig().getString(SPARK_MASTER);
        String sparkJars = jobParameter.getApplicationConfig().getString(SPARK_JARS);
        SparkSession spark =
                SparkSession.builder()
                        .appName(applicationName)
                        .master(sparkMaster)
                        .config(SPARK_JARS, sparkJars)
                        .getOrCreate();
        this.spark = spark;
    }

    public void stopSparkSession() {
        this.spark.stop();
    }

    private static final Set<String> unwantedAboutValues =
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

    private StructType getSchema() {
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
                                "left_outer");
        finalMergedDF =
                finalMergedDF.selectExpr("a.subject", "map_concat(a.object, b.object) as object");
        JavaPairRDD<Long, ChebiEntry> chebiEntryPairRDD =
                finalMergedDF
                        .toJavaRDD()
                        .mapToPair(new ChebiEntryMapper())
                        .filter(pair -> pair != null && pair._1 != null && pair._2 != null);
        return chebiEntryPairRDD;
    }

    protected static JavaRDD<Row> getAboutJavaRDDFromDescription(
            StructType schema, JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedAboutRDFDescriptionsRDD =
                rdfDescriptionsRDD
                        .flatMap(
                                row -> {
                                    String currentSubject = null;
                                    Map<String, Seq<String>> processedAttributes =
                                            new LinkedHashMap<>();
                                    String aboutValue = row.getString(row.fieldIndex("_rdf:about"));
                                    if (!unwantedAboutValues.contains(aboutValue)) {
                                        currentSubject = aboutValue;
                                    }
                                    if (currentSubject != null) {
                                        for (String key : schema.fieldNames()) {
                                            if (!key.equals("_rdf:about")
                                                    && !key.equals("_rdf:nodeID")) {
                                                List<String> values = null;
                                                if (key.equals("chebiStructuredName")
                                                        || key.equals("rdfs:subClassOf")) {
                                                    Object resourceObj =
                                                            row.get(row.fieldIndex(key));
                                                    if (resourceObj instanceof AbstractSeq) {
                                                        AbstractSeq<Row> resourceArraySeq =
                                                                (AbstractSeq<Row>) resourceObj;
                                                        List<Row> resourceList =
                                                                JavaConverters.seqAsJavaList(
                                                                        resourceArraySeq);
                                                        values =
                                                                resourceList.stream()
                                                                        .map(
                                                                                resourceRow -> {
                                                                                    String
                                                                                            resourceId =
                                                                                                    null;
                                                                                    String nodeId =
                                                                                            null;
                                                                                    if (Arrays
                                                                                            .asList(
                                                                                                    resourceRow
                                                                                                            .schema()
                                                                                                            .fieldNames())
                                                                                            .contains(
                                                                                                    "_rdf:resource")) {
                                                                                        resourceId =
                                                                                                resourceRow
                                                                                                        .getString(
                                                                                                                resourceRow
                                                                                                                        .fieldIndex(
                                                                                                                                "_rdf:resource"));
                                                                                    }
                                                                                    if (Arrays
                                                                                            .asList(
                                                                                                    resourceRow
                                                                                                            .schema()
                                                                                                            .fieldNames())
                                                                                            .contains(
                                                                                                    "_rdf:nodeID")) {
                                                                                        nodeId =
                                                                                                resourceRow
                                                                                                        .getString(
                                                                                                                resourceRow
                                                                                                                        .fieldIndex(
                                                                                                                                "_rdf:nodeID"));
                                                                                    }
                                                                                    return resourceId
                                                                                                    != null
                                                                                            ? resourceId
                                                                                            : nodeId;
                                                                                })
                                                                        .collect(
                                                                                Collectors
                                                                                        .toList());
                                                    } else if (resourceObj instanceof String) {
                                                        String resourceId = null;
                                                        String nodeId = null;
                                                        if (Arrays.asList(row.schema().fieldNames())
                                                                .contains("_rdf:resource")) {
                                                            resourceId =
                                                                    row.getString(
                                                                            row.fieldIndex(
                                                                                    "_rdf:resource"));
                                                        }
                                                        if (Arrays.asList(row.schema().fieldNames())
                                                                .contains("_rdf:nodeID")) {
                                                            nodeId =
                                                                    row.getString(
                                                                            row.fieldIndex(
                                                                                    "_rdf:nodeID"));
                                                        }
                                                        String value =
                                                                resourceId != null
                                                                        ? resourceId
                                                                        : nodeId;
                                                        values = Collections.singletonList(value);
                                                    }
                                                } else if (key.equals("oboInOwl:hasDbXref")) {
                                                    values = row.getList(row.fieldIndex(key));
                                                } else {
                                                    Object valueObj = row.get(row.fieldIndex(key));
                                                    if (valueObj instanceof String) {
                                                        String stringValue = (String) valueObj;
                                                        if (!stringValue.isEmpty()) {
                                                            values =
                                                                    Collections.singletonList(
                                                                            stringValue);
                                                        }
                                                    } else if (valueObj instanceof List) {
                                                        List<String> valueList =
                                                                (List<String>) valueObj;
                                                        if (valueList != null
                                                                && !valueList.isEmpty()) {
                                                            values = valueList;
                                                        }
                                                    }
                                                }
                                                if (values != null) {
                                                    processedAttributes.put(
                                                            key,
                                                            JavaConverters.asScalaBuffer(values));
                                                }
                                            }
                                        }
                                        Row newRow =
                                                RowFactory.create(
                                                        currentSubject,
                                                        JavaConverters.mapAsScalaMap(
                                                                processedAttributes));
                                        return Collections.singletonList(newRow).iterator();
                                    }
                                    return Collections.emptyIterator();
                                })
                        .filter(Objects::nonNull);
        return processedAboutRDFDescriptionsRDD;
    }

    protected static JavaRDD<Row> getNodeIdJavaRDDFromDescription(
            StructType schema, JavaRDD<Row> rdfDescriptionsRDD) {
        JavaRDD<Row> processedNodeIdRDFDescriptionsRDD =
                rdfDescriptionsRDD
                        .flatMap(
                                row -> {
                                    List<String> commonTypeValue = new ArrayList<>();
                                    String currentSubject =
                                            row.getString(row.fieldIndex("_rdf:nodeID"));
                                    if (currentSubject != null) {
                                        Map<String, Seq<String>> processedAttributes =
                                                new LinkedHashMap<>();
                                        for (String key : schema.fieldNames()) {
                                            if (!key.equals("_rdf:about")
                                                    && !key.equals("_rdf:nodeID")) {
                                                List<String> values = null;
                                                if (key.equals("rdf:type")) {
                                                    Object resourceObj =
                                                            row.get(row.fieldIndex(key));
                                                    if (resourceObj instanceof AbstractSeq) {
                                                        AbstractSeq<Row> resourceArraySeq =
                                                                (AbstractSeq<Row>) resourceObj;
                                                        List<Row> resourceList =
                                                                JavaConverters.seqAsJavaList(
                                                                        resourceArraySeq);
                                                        commonTypeValue =
                                                                resourceList.stream()
                                                                        .map(
                                                                                resourceRow -> {
                                                                                    String
                                                                                            commonType =
                                                                                                    "";
                                                                                    if (Arrays
                                                                                            .asList(
                                                                                                    resourceRow
                                                                                                            .schema()
                                                                                                            .fieldNames())
                                                                                            .contains(
                                                                                                    "_rdf:resource")) {
                                                                                        String
                                                                                                type =
                                                                                                        resourceRow
                                                                                                                .getString(
                                                                                                                        resourceRow
                                                                                                                                .fieldIndex(
                                                                                                                                        "_rdf:resource"));
                                                                                        if (type
                                                                                                .contains(
                                                                                                        "ChEBI_Common_Name")) {
                                                                                            commonType =
                                                                                                    resourceRow
                                                                                                            .getString(
                                                                                                                    resourceRow
                                                                                                                            .fieldIndex(
                                                                                                                                    "_rdf:resource"));
                                                                                        }
                                                                                    }
                                                                                    return commonType;
                                                                                })
                                                                        .collect(
                                                                                Collectors
                                                                                        .toList());
                                                        commonTypeValue.removeIf(String::isEmpty);
                                                    }
                                                }
                                                if (key.equals("rdfs:label")) {
                                                    if (commonTypeValue.size() > 0) {
                                                        values = row.getList(row.fieldIndex(key));
                                                        processedAttributes.put(
                                                                "name",
                                                                JavaConverters.asScalaBuffer(
                                                                        values));
                                                        values = null;
                                                        commonTypeValue = new ArrayList<>();
                                                    } else {
                                                        values = row.getList(row.fieldIndex(key));
                                                    }
                                                } else if (key.equals("owl:onProperty")
                                                        || key.equals("owl:someValuesFrom")) {
                                                    Object resourceObj =
                                                            row.get(row.fieldIndex(key));
                                                    if (resourceObj instanceof AbstractSeq) {
                                                        AbstractSeq<Row> resourceArraySeq =
                                                                (AbstractSeq<Row>) resourceObj;
                                                        List<Row> resourceList =
                                                                JavaConverters.seqAsJavaList(
                                                                        resourceArraySeq);
                                                        values =
                                                                resourceList.stream()
                                                                        .map(
                                                                                resourceRow -> {
                                                                                    String
                                                                                            resourceId =
                                                                                                    null;
                                                                                    if (Arrays
                                                                                            .asList(
                                                                                                    resourceRow
                                                                                                            .schema()
                                                                                                            .fieldNames())
                                                                                            .contains(
                                                                                                    "_rdf:resource")) {
                                                                                        resourceId =
                                                                                                resourceRow
                                                                                                        .getString(
                                                                                                                resourceRow
                                                                                                                        .fieldIndex(
                                                                                                                                "_rdf:resource"));
                                                                                    }
                                                                                    return resourceId;
                                                                                })
                                                                        .collect(
                                                                                Collectors
                                                                                        .toList());
                                                    }
                                                }
                                                if (values != null) {
                                                    processedAttributes.put(
                                                            key,
                                                            JavaConverters.asScalaBuffer(values));
                                                }
                                            }
                                        }
                                        Row newRow =
                                                RowFactory.create(
                                                        currentSubject,
                                                        JavaConverters.mapAsScalaMap(
                                                                processedAttributes));
                                        return Collections.singletonList(newRow).iterator();
                                    }

                                    return Collections.emptyIterator();
                                })
                        .filter(Objects::nonNull);
        return processedNodeIdRDFDescriptionsRDD;
    }

    protected static Dataset<Row> getLabelAndClassColumnsFromAboutRDD(
            StructType explodedSchema, Dataset<Row> processedAboutDF) {
        Dataset<Row> explodedAboutDF =
                processedAboutDF
                        .selectExpr("subject AS about_subject", "object")
                        .flatMap(
                                (FlatMapFunction<Row, Row>)
                                        row -> {
                                            List<Row> results = new ArrayList<>();
                                            String subject = row.getAs("about_subject");
                                            scala.collection.Map<Object, Object> objectRow =
                                                    row.getMap(1);
                                            List<String> chebiStructuredNames =
                                                    objectRow.contains("chebiStructuredName")
                                                            ? JavaConverters.seqAsJavaListConverter(
                                                                            (Seq<String>)
                                                                                    objectRow
                                                                                            .get(
                                                                                                    "chebiStructuredName")
                                                                                            .get())
                                                                    .asJava()
                                                            : null;
                                            List<String> subClassOfs =
                                                    objectRow.contains("rdfs:subClassOf")
                                                            ? JavaConverters.seqAsJavaListConverter(
                                                                            (Seq<String>)
                                                                                    objectRow
                                                                                            .get(
                                                                                                    "rdfs:subClassOf")
                                                                                            .get())
                                                                    .asJava()
                                                            : null;
                                            if (chebiStructuredNames != null) {
                                                for (String chebiStructuredName :
                                                        chebiStructuredNames) {
                                                    results.add(
                                                            RowFactory.create(
                                                                    subject,
                                                                    chebiStructuredName,
                                                                    null));
                                                }
                                            }
                                            if (subClassOfs != null) {
                                                for (String subClassOf : subClassOfs) {
                                                    results.add(
                                                            RowFactory.create(
                                                                    subject, null, subClassOf));
                                                }
                                            }
                                            return results.iterator();
                                        },
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
                                "right_outer")
                        .select(col("a.about_subject"), col("b.subject"), col("b.object"))
                        .toJavaRDD()
                        .flatMap(
                                (FlatMapFunction<Row, Tuple2<String, Map<String, Seq<String>>>>)
                                        row -> {
                                            List<Tuple2<String, Map<String, Seq<String>>>> result =
                                                    new ArrayList<>();
                                            String aboutSubject = row.getString(0);
                                            Map<String, Seq<String>> objectMap =
                                                    JavaConverters.mapAsJavaMap(row.getMap(2));
                                            for (Map.Entry<String, Seq<String>> entry :
                                                    objectMap.entrySet()) {
                                                HashMap<String, Seq<String>> valueMap =
                                                        new HashMap<>();
                                                valueMap.put(entry.getKey(), entry.getValue());
                                                result.add(new Tuple2<>(aboutSubject, valueMap));
                                            }
                                            return result.iterator();
                                        })
                        .mapToPair(
                                (PairFunction<
                                                Tuple2<String, Map<String, Seq<String>>>,
                                                String,
                                                Map<String, Seq<String>>>)
                                        tuple2 -> new Tuple2<>(tuple2._1, tuple2._2))
                        .reduceByKey(
                                (map1, map2) -> {
                                    for (Map.Entry<String, Seq<String>> entry : map2.entrySet()) {
                                        String key = entry.getKey();
                                        Seq<String> value = entry.getValue();
                                        if (map1.containsKey(key)) {
                                            List<String> combinedValue =
                                                    new ArrayList<>(
                                                            JavaConverters.seqAsJavaList(
                                                                    map1.get(key)));
                                            combinedValue.addAll(
                                                    JavaConverters.seqAsJavaList(value));
                                            map1.put(
                                                    key,
                                                    JavaConverters.asScalaBuffer(combinedValue)
                                                            .toSeq());
                                        } else {
                                            map1.put(key, value);
                                        }
                                    }
                                    return map1;
                                })
                        .map(
                                row ->
                                        RowFactory.create(
                                                row._1, JavaConverters.mapAsScalaMap(row._2)));
        return joinedNodeRDD;
    }
}
