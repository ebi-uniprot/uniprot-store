package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ProteomeXMLSchemaProvider {
    public static final String UPID = "upid";
    public static final String UPID_ATTRIBUTE = "_upid";
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
    public static final String VALUE_LOWER = "_value";
    public static final String CONSORTIUM = "consortium";
    public static final String ID = "_id";
    public static final String ISOLATE = "isolate";
    public static final String REDUNDANT_TO = "redundantTo";
    public static final String PANPROTEOME = "panproteome";
    public static final String REDUNDANT_PROTEOME = "redundantProteome";
    public static final String EXCLUDED = "excluded";
    public static final String SIMILARITY = "_similarity";
    public static final String EXCLUSION_REASON = "exclusionReason";

    private ProteomeXMLSchemaProvider() {}

    public static StructType getProteomeXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add(PROTEIN_COUNT, DataTypes.StringType, true);
        structType = structType.add(UPID, DataTypes.StringType, false);
        structType = structType.add(TAXONOMY, DataTypes.LongType, false);
        structType = structType.add(IS_REFERENCE_PROTEOME, DataTypes.BooleanType, false);
        structType = structType.add(IS_REPRESENTATIVE_PROTEOME, DataTypes.BooleanType, false);
        structType = structType.add(MODIFIED, DataTypes.DateType, false);
        structType = structType.add(STRAIN, DataTypes.StringType, true);
        structType = structType.add(DESCRIPTION, DataTypes.StringType, true);
        structType = structType.add(ISOLATE, DataTypes.StringType, true);
        structType = structType.add(REDUNDANT_TO, DataTypes.StringType, true);
        structType = structType.add(PANPROTEOME, DataTypes.StringType, true);
        structType = structType.add(ANNOTATION_SCORE, getAnnotationScoreSchema(), true);
        structType = structType.add(GENOME_ANNOTATION, getGenomeAnnotationSchema(), true);
        structType = structType.add(GENOME_ASSEMBLY, getGenomeAssemblySchema(), true);
        structType =
                structType.add(COMPONENT, DataTypes.createArrayType(getComponentSchema()), false);
        structType = structType.add(SCORES, DataTypes.createArrayType(getScoresSchema()), true);
        structType =
                structType.add(REFERENCE, DataTypes.createArrayType(getReferenceSchema()), true);
        structType =
                structType.add(
                        REDUNDANT_PROTEOME,
                        DataTypes.createArrayType(getRedundantProteomeSchema()),
                        true);
        structType =
                structType.add(EXCLUDED, DataTypes.createArrayType(getExclusionSchema()), true);
        return structType;
    }

    public static StructType getExclusionSchema() {
        StructType exclusion = new StructType();
        exclusion =
                exclusion.add(
                        EXCLUSION_REASON, DataTypes.createArrayType(DataTypes.StringType), false);
        return exclusion;
    }

    public static StructType getRedundantProteomeSchema() {
        StructType redundantProtein = new StructType();
        redundantProtein = redundantProtein.add(UPID_ATTRIBUTE, DataTypes.StringType, false);
        redundantProtein = redundantProtein.add(SIMILARITY, DataTypes.StringType, true);
        return redundantProtein;
    }

    public static StructType getReferenceSchema() {
        StructType reference = new StructType();
        reference = reference.add(CITATION, getCitationSchema(), false);
        return reference;
    }

    public static StructType getCitationSchema() {
        StructType citation = new StructType();
        citation = citation.add(TYPE, DataTypes.StringType, false);
        citation = citation.add(DATE, DataTypes.StringType, true);
        citation = citation.add(DB, DataTypes.StringType, true);
        citation = citation.add(FIRST, DataTypes.StringType, true);
        citation = citation.add(LAST, DataTypes.StringType, true);
        citation = citation.add(NAME, DataTypes.StringType, true);
        citation = citation.add(VOLUME, DataTypes.StringType, true);
        citation = citation.add(TITLE, DataTypes.StringType, true);
        citation = citation.add(AUTHOR_LIST, getAuthorListScheme(), true);
        citation =
                citation.add(DB_REFERENCE, DataTypes.createArrayType(getDbReferenceScheme()), true);
        return citation;
    }

    public static StructType getDbReferenceScheme() {
        StructType dbReference = new StructType();
        dbReference = dbReference.add(VALUE, DataTypes.StringType, true);
        dbReference = dbReference.add(ID, DataTypes.StringType, false);
        dbReference = dbReference.add(TYPE, DataTypes.StringType, false);
        return dbReference;
    }

    public static StructType getAuthorListScheme() {
        StructType author = new StructType();
        author = author.add(PERSON, DataTypes.createArrayType(getPersonScheme()), true);
        author = author.add(CONSORTIUM, DataTypes.createArrayType(getConsortiumSchema()), true);
        return author;
    }

    public static StructType getConsortiumSchema() {
        StructType consortium = new StructType();
        consortium = consortium.add(VALUE, DataTypes.StringType, true);
        consortium = consortium.add(NAME, DataTypes.StringType, false);
        return consortium;
    }

    public static StructType getPersonScheme() {
        StructType person = new StructType();
        person = person.add(VALUE, DataTypes.StringType, true);
        person = person.add(NAME, DataTypes.StringType, false);
        return person;
    }

    public static StructType getScoresSchema() {
        StructType scores = new StructType();
        scores = scores.add(NAME, DataTypes.StringType, false);
        scores = scores.add(PROPERTY, DataTypes.createArrayType((getPropertySchema()), true));
        return scores;
    }

    public static StructType getPropertySchema() {
        StructType property = new StructType();
        property = property.add(VALUE, DataTypes.StringType, true);
        property = property.add(NAME, DataTypes.StringType, false);
        property = property.add(VALUE_LOWER, DataTypes.StringType, false);
        return property;
    }

    public static StructType getComponentSchema() {
        StructType component = new StructType();
        component = component.add(NAME, DataTypes.StringType, false);
        component = component.add(PROTEIN_COUNT, DataTypes.LongType, true);
        component = component.add(BIO_SAMPLE_ID, DataTypes.StringType, true);
        component = component.add(DESCRIPTION, DataTypes.StringType, true);
        component =
                component.add(
                        GENOME_ACCESSION, DataTypes.createArrayType(DataTypes.StringType), true);
        component = component.add(GENOME_ANNOTATION, getGenomeAnnotationSourceSchema(), true);
        return component;
    }

    public static StructType getGenomeAnnotationSourceSchema() {
        StructType genomeAnnotationSource = new StructType();
        genomeAnnotationSource =
                genomeAnnotationSource.add(GENOME_ANNOTATION_SOURCE, DataTypes.StringType, false);
        return genomeAnnotationSource;
    }

    public static StructType getGenomeAssemblySchema() {
        StructType genomeAssembly = new StructType();
        genomeAssembly = genomeAssembly.add(GENOME_ASSEMBLY, DataTypes.StringType, true);
        genomeAssembly = genomeAssembly.add(GENOME_ASSEMBLY_URL, DataTypes.StringType, true);
        genomeAssembly = genomeAssembly.add(GENOME_ASSEMBLY_SOURCE, DataTypes.StringType, false);
        genomeAssembly = genomeAssembly.add(GENOME_REPRESENTATION, DataTypes.StringType, true);
        return genomeAssembly;
    }

    public static StructType getAnnotationScoreSchema() {
        StructType annotationScore = new StructType();
        annotationScore = annotationScore.add(VALUE, DataTypes.StringType, true);
        annotationScore =
                annotationScore.add(NORMALIZED_ANNOTATION_SCORE, DataTypes.LongType, false);
        return annotationScore;
    }

    public static StructType getGenomeAnnotationSchema() {
        StructType genomeAnnotation = new StructType();
        genomeAnnotation =
                genomeAnnotation.add(GENOME_ANNOTATION_SOURCE, DataTypes.StringType, false);
        genomeAnnotation = genomeAnnotation.add(GENOME_ANNOTATION_URL, DataTypes.StringType, true);
        return genomeAnnotation;
    }
}
