package org.uniprot.store.spark.indexer.proteome;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.proteome.converter.DatasetProteomeEntryConverter;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomEntryToPair;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

/**
 * @author sahmad
 * @created 21/08/2020
 */
public class ProteomeRDDReader implements PairRDDReader<String, ProteomeEntry> {
    public static final String UPID = "upid";
    public static final String TAXONOMY = "taxonomy";
    public static final String STRAIN = "strain";
    public static final String MODIFIED = "modified";
    public static final String IS_REFERENCE_PROTEOME = "isReferenceProteome";
    public static final String IS_REPRESENTATIVE_PROTEOME = "isRepresentativeProteome";
    public static final String GENOME_ANNOTATION = "genomeAnnotation";
    public static final String GENOME_ANNOTATION_SOURCE = "genomeAnnotationSource";
    public static final String GENOME_ANNOTATION_URL = "genomeAnnotationUrl";
    public static final String GENOME_ASSEMBLY = "genomeAssembly";
    public static final String GENOME_ASSEMBLY_SOURCE = "genomeAssemblySource";
    public static final String GENOME_ASSEMBLY_URL = "genomeAssemblyUrl";
    public static final String GENOME_REPRESENTATION = "genomeRepresentation";
    public static final String COMPONENT = "component";
    public static final String NAME = "_name";
    public static final String PROTEIN_COUNT = "_proteinCount";
    public static final String DESCRIPTION = "description";
    public static final String ANNOTATION_SCORE = "annotationScore";
    public static final String NORMALIZED_ANNOTATION_SCORE = "_normalizedAnnotationScore";
    public static final String REFERENCE = "reference";
    public static final String CITATION = "citation";
    public static final String TYPE = "_type";
    public static final String DATE = "_date";
    public static final String TITLE = "title";
    public static final String AUTHOR_LIST = "authorList";
    public static final String PERSON = "person";
    public static final String DB_REFERENCE = "dbReference";
    public static final String FIRST = "_first";
    public static final String LAST = "_last";
    public static final String VOLUME = "_volume";
    public static final String DB = "_db";
    public static final String SCORES = "scores";
    public static final String VALUE = "_VALUE";
    public static final String BIO_SAMPLE_ID = "biosampleId";
    public static final String GENOME_ACCESSION = "genomeAccession";
    public static final String PROPERTY = "property";
    public static final String VALUE_LOWER = "value";
    public static final String CONSORTIUM = "consortium";
    public static final String ID = "_id";

    private final JobParameter jobParameter;
    private final boolean shouldRepartition;

    public ProteomeRDDReader(JobParameter jobParameter, boolean shouldRepartition) {
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    @Override
    public JavaPairRDD<String, ProteomeEntry> load() {
        Config config = jobParameter.getApplicationConfig();
        int repartition = Integer.parseInt(config.getString("proteome.repartition"));

        JavaRDD<Row> proteomeEntryDataset = loadRawXml().toJavaRDD();
        if (this.shouldRepartition && repartition > 0) {
            proteomeEntryDataset = proteomeEntryDataset.repartition(repartition);
        }

        return proteomeEntryDataset
                .map(new DatasetProteomeEntryConverter())
                .mapToPair(new ProteomEntryToPair());
    }

    private Dataset<Row> loadRawXml() {
        Config config = jobParameter.getApplicationConfig();
        SparkSession spark =
                SparkSession.builder()
                        .config(jobParameter.getSparkContext().getConf())
                        .getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String xmlFilePath = releaseInputDir + config.getString("proteome.xml.file");
        Dataset<Row> data =
                spark.read()
                        .format("com.databricks.spark.xml")
                        .option("rowTag", "proteome")
                        .schema(geProteomeXMLSchema())
                        .load(xmlFilePath);
        data.printSchema();
        return data;
    }

    public static StructType geProteomeXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add(UPID, DataTypes.StringType, true);
        structType = structType.add(TAXONOMY, DataTypes.LongType, true);
        structType = structType.add(IS_REFERENCE_PROTEOME, DataTypes.BooleanType, true);
        structType = structType.add(IS_REPRESENTATIVE_PROTEOME, DataTypes.BooleanType, true);
        structType = structType.add(MODIFIED, DataTypes.StringType, true);
        structType = structType.add(STRAIN, DataTypes.StringType, true);
        structType = structType.add(DESCRIPTION, DataTypes.StringType, true);
        structType = structType.add(ANNOTATION_SCORE, getAnnotationScoreSchema(), true);
        structType = structType.add(GENOME_ANNOTATION, getGenomeAnnotationSchema(), true);
        structType = structType.add(GENOME_ASSEMBLY, getGenomeAssemblySchema(), true);
        structType = structType.add(COMPONENT, DataTypes.createArrayType(getComponentSchema()), true);
        structType = structType.add(SCORES, DataTypes.createArrayType(getScoresSchema()), true);
        structType = structType.add(REFERENCE, DataTypes.createArrayType(getReferenceSchema()), true);
        return structType;
    }

    public static StructType getReferenceSchema() {
        StructType reference = new StructType();
        reference = reference.add(CITATION, getCitationSchema(), true);
        return reference;
    }

    public static StructType getCitationSchema() {
        StructType citation = new StructType();
        citation = citation.add(DATE, DataTypes.StringType, true);
        citation = citation.add(DB, DataTypes.StringType, true);
        citation = citation.add(FIRST, DataTypes.StringType, true);
        citation = citation.add(LAST, DataTypes.StringType, true);
        citation = citation.add(NAME, DataTypes.StringType, true);
        citation = citation.add(TYPE, DataTypes.StringType, true);
        citation = citation.add(VOLUME, DataTypes.StringType, true);
        citation = citation.add(TITLE, DataTypes.StringType, true);
        citation = citation.add(AUTHOR_LIST, getAuthorScheme(), true);
        citation = citation.add(DB_REFERENCE, DataTypes.createArrayType(getDbReferenceScheme()), true);
        return citation;
    }

    public static StructType getDbReferenceScheme() {
        StructType dbReference = new StructType();
        dbReference = dbReference.add(VALUE, DataTypes.StringType, true);
        dbReference = dbReference.add(ID, DataTypes.StringType, true);
        dbReference = dbReference.add(TYPE, DataTypes.StringType, true);
        return dbReference;
    }

    public static StructType getAuthorScheme() {
        StructType author = new StructType();
        author = author.add(PERSON, DataTypes.createArrayType(getPersonScheme()), true);
        author = author.add(CONSORTIUM, getConsortiumSchema(), true);
        return author;
    }

    public static StructType getConsortiumSchema() {
        StructType consortium = new StructType();
        consortium = consortium.add(VALUE, DataTypes.StringType, true);
        consortium = consortium.add(NAME, DataTypes.StringType, true);
        return consortium;
    }

    public static StructType getPersonScheme() {
        StructType person = new StructType();
        person = person.add(VALUE, DataTypes.StringType, true);
        person = person.add(NAME, DataTypes.StringType, true);
        return person;
    }

    public static StructType getScoresSchema() {
        StructType scores = new StructType();
        scores = scores.add(NAME, DataTypes.StringType, true);
        scores = scores.add(PROPERTY, DataTypes.createArrayType((getPropertySchema()), true));
        return scores;
    }

    private static StructType getPropertySchema() {
        StructType property = new StructType();
        property = property.add(VALUE, DataTypes.StringType, true);
        property = property.add(NAME, DataTypes.StringType, true);
        property = property.add(VALUE_LOWER, DataTypes.StringType, true);
        return property;
    }

    public static StructType getComponentSchema() {
        StructType component = new StructType();
        component = component.add(NAME, DataTypes.StringType, true);
        component = component.add(PROTEIN_COUNT, DataTypes.LongType, true);
        component = component.add(BIO_SAMPLE_ID, DataTypes.StringType, true);
        component = component.add(DESCRIPTION, DataTypes.StringType, true);
        component = component.add(GENOME_ACCESSION, DataTypes.StringType, true);
        component = component.add(GENOME_ANNOTATION, getGenomeAnnotationSourceSchema(), true);
        return component;
    }

    public static StructType getGenomeAnnotationSourceSchema() {
        StructType genomeAnnotationSource = new StructType();
        genomeAnnotationSource = genomeAnnotationSource.add(GENOME_ANNOTATION_SOURCE, DataTypes.StringType, true);
        return genomeAnnotationSource;
    }

    public static StructType getGenomeAssemblySchema() {
        StructType genomeAssembly = new StructType();
        genomeAssembly = genomeAssembly.add(GENOME_ASSEMBLY, DataTypes.StringType, true);
        genomeAssembly = genomeAssembly.add(GENOME_ASSEMBLY_URL, DataTypes.StringType, true);
        genomeAssembly = genomeAssembly.add(GENOME_ASSEMBLY_SOURCE, DataTypes.StringType, true);
        genomeAssembly = genomeAssembly.add(GENOME_REPRESENTATION, DataTypes.StringType, true);
        return genomeAssembly;
    }

    public static StructType getAnnotationScoreSchema() {
        StructType annotationScore = new StructType();
        annotationScore = annotationScore.add(VALUE, DataTypes.StringType, true);
        annotationScore = annotationScore.add(NORMALIZED_ANNOTATION_SCORE, DataTypes.LongType, true);
        return annotationScore;
    }

    public static StructType getGenomeAnnotationSchema() {
        StructType genomeAnnotation = new StructType();
        genomeAnnotation = genomeAnnotation.add(GENOME_ANNOTATION_SOURCE, DataTypes.StringType, true);
        genomeAnnotation = genomeAnnotation.add(GENOME_ANNOTATION_URL, DataTypes.StringType, true);
        return genomeAnnotation;
    }
}
