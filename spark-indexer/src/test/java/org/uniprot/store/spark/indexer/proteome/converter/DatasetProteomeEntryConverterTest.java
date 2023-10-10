package org.uniprot.store.spark.indexer.proteome.converter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.uniprot.core.citation.SubmissionDatabase.EMBL_GENBANK_DDBJ;
import static org.uniprot.core.proteome.CPDStatus.STANDARD;
import static org.uniprot.store.spark.indexer.proteome.ProteomeXMLSchemaProvider.*;

import java.sql.Date;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.*;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.proteome.*;
import org.uniprot.core.proteome.impl.*;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;

class DatasetProteomeEntryConverterTest {

    private static final String EXCLUSION_REASON_VAL = "missing tRNA genes";
    private static final String UP_ID_VAL = "UP000000718";
    private static final long TAXONOMY_VAL = 289376;
    private static final boolean IS_REFERENCE_PROTEOME_VAL = true;
    private static final boolean IS_REPRESENTATIVE_PROTEOME_VAL = false;
    private static final String STRAIN_VAL = "ATCC 51303 / DSM 11347 / YP87<";
    private static final String DESCRIPTION_VAL =
            "Thermodesulfovibrio yellowstonii (strain ATCC 51303 / DSM 11347 / YP87) is a thermophilic sulfate-reducing bacterium isolated from a thermal vent in Yellowstone Lake in Wyoming, USA. It has the ability to use sulfate, thiosulfate, and sulfite as terminal electron acceptors. Pyruvate can support fermentative growth";
    private static final String GENOME_ANNOTATION_SOURCE_VAL = "ENA/EMBL";
    private static final String GENOME_ASSEMBLY_SOURCE_VAL = "ENA/EMBL";
    private static final String GENOME_ANNOTATION_URL_VAL =
            "https://www.ebi.ac.uk/ena/browser/view/GCA_000020985.1";
    private static final String GENOME_ASSEMBLY_URL_VAL =
            "https://www.ebi.ac.uk/ena/browser/view/GCA_000020985.1";
    private static final String ANNOTATION_SCORE_VAL = "Score";
    private static final long ANNOTATION_SCORE_SCORE_VAL = 1267L;
    private static final String GENOME_ASSEMBLY_ID_VAL = "GCA_000020985.1";
    private static final String GENOME_ASSEMBLY_REPRESENTATION_VAL = "full";
    private static final String COMPONENT_NAME_0 = "Chromosome";
    private static final long COMPONENT_PROTEIN_COUNT_0 = 10L;
    private static final String COMPONENT_BIO_SAMPLE_ID_0 = "SAMN02603929";
    private static final String COMPONENT_DESCRIPTION_0 =
            "Thermodesulfovibrio yellowstonii DSM 11347";
    private static final String COMPONENT_GENOME_ACCESSION_0 = "CP001147";
    private static final String COMPONENT_SOURCE_0 = "ENA/EMBL";
    private static final String COMPONENT_NAME_1 = "Genome";
    private static final long COMPONENT_PROTEIN_COUNT_1 = 11L;
    private static final String COMPONENT_BIO_SAMPLE_ID_1 = "SAMN02603927";
    private static final String COMPONENT_DESCRIPTION_1 = "Lelystad virus";
    private static final String COMPONENT_GENOME_ACCESSION_1 = "M96262";
    private static final String COMPONENT_SOURCE_1 = "EBI/EMBL";
    private static final String CITATION_DATE_0 = "1998";
    private static final String CITATION_DB_0 = "";
    private static final String CITATION_FIRST_0 = "173";
    private static final String CITATION_LAST_0 = "180";
    private static final String CITATION_NAME_0 = "Arch. Virol.";
    private static final String CITATION_TYPE_0 = "journal article";
    private static final String CITATION_VOLUME_0 = "143";
    private static final String CITATION_TITLE_0 =
            "Molecular cloning and complete nucleotide sequence of galinsoga mosaic virus genomic RNA.";
    private static final String CITATION_DATE_1 = "2008-08";
    private static final String CITATION_DB_1 = "EMBL/GenBank/DDBJ databases";
    private static final String CITATION_FIRST_1 = "0";
    private static final String CITATION_LAST_1 = "0";
    private static final String CITATION_NAME_1 = "";
    private static final String CITATION_TYPE_1 = "submission";
    private static final String CITATION_VOLUME_1 = "0";
    private static final String CITATION_TITLE_1 =
            "The complete genome sequence of Thermodesulfovibrio yellowstonii strain ATCC 51303 / DSM 11347 / YP87.";
    private static final String CITATION_FIRST_2 = "23999";
    private static final String CITATION_LAST_2 = "99993333";
    private static final String CITATION_TYPE_2 = "book";
    private static final String CITATION_VOLUME_2 = "1000";
    private static final String CITATION_TYPE_3 = "patent";
    private static final String CITATION_TYPE_4 = "thesis";
    private static final String CITATION_TYPE_5 = "online journal article";
    private static final String CITATION_TYPE_6 = "UniProt indexed literatures";
    private static final String CITATION_TYPE_7 = "unpublished observations";
    private static final String AUTHOR_NAME_0_0 = "Briddon R.W.";
    private static final String CONSORTIUM_NAME_0_0 = "Rat Genome";
    private static final String CONSORTIUM_NAME_0_1 = "Cat Genome";
    private static final String CONSORTIUM_NAME_1_0 = "Dog Genome";
    private static final String CONSORTIUM_NAME_1_1 = "Elephant Genome";
    private static final String AUTHOR_NAME_1_0 = "Heydarnejad J.";
    private static final String AUTHOR_NAME_0_1 = "Khosrowfar F.";
    private static final String AUTHOR_NAME_1_1 = "Massumi H.";
    private static final String DB_REF_VALUE_0_0 = "25635016";
    private static final String DB_REF_VALUE_0_1 = "10.1016/j.virusres.2010.05.016";
    private static final String DB_REF_VALUE_1_0 = "10.1006/viro.1993.1008";
    private static final String DB_REF_VALUE_1_1 = "20566344";
    private static final String DB_REF_NAME_0_0 = "PubMed";
    private static final String DB_REF_NAME_1_0 = "DOI";
    private static final String DB_REF_NAME_1_1 = "PubMed";
    private static final String DB_REF_NAME_0_1 = "DOI";
    private static final String SCORES_NAME_0 = "cpd";
    private static final String SCORES_NAME_1 = "abc";
    private static final String SCORES_SCORE_0_0 = "confidence";
    private static final String SCORES_VALUE_0_0 = "1";
    private static final String SCORES_SCORE_0_1 = "confidence";
    private static final String SCORES_VALUE_0_1 = "3";
    private static final String SCORES_SCORE_1_0 = "status";
    private static final String SCORES_SCORE_1_1 = "status";
    private static final String SCORES_SCORE_2_0 = "averageCds";
    private static final String SCORES_SCORE_2_1 = "averageCds";
    private static final String SCORES_VALUE_1_0 = "Standard";
    private static final String SCORES_VALUE_1_1 = "Advanced";
    private static final String SCORES_VALUE_2_0 = "5";
    private static final String SCORES_VALUE_2_1 = "1";
    private static final long EPOCH_MILLI_NOW = Instant.now().toEpochMilli();
    private static final String ISOLATE_VAL = "someIsolate";
    private static final String REDUNDANT_TO_VAL = "209";
    private static final String PANPROTEOME_VAL = "9450";
    private static final String REDUNDANT_PROTEIN_ID_0 = "2098";
    private static final String REDUNDANT_PROTEIN_ID_1 = "1634";
    private static final String REDUNDANT_PROTEIN_SIMILARITY_0 = "23.5";
    private static final String REDUNDANT_PROTEIN_SIMILARITY_1 = "443.5";
    public static final String PROTEIN_COUNT_VALUE = "25";
    private final DatasetProteomeEntryConverter proteomeEntryConverter =
            new DatasetProteomeEntryConverter();

    @Test
    void fullProteomeEntry() throws Exception {
        Row row = getFullProteomeRow();

        ProteomeEntry result = proteomeEntryConverter.call(row);

        assertThat(result, samePropertyValuesAs(getExpectedFullResult()));
    }

    @Test
    void requiredOnly() throws Exception {
        Row row = getMinimalProteomeRow();

        ProteomeEntry result = proteomeEntryConverter.call(row);

        assertThat(result, samePropertyValuesAs(getExpectedMinimalResult()));
    }

    @Test
    void invalidCitationType_throwsException() {
        try {
            Row row = getProteomeRowWithInvalidCitationType();

            proteomeEntryConverter.call(row);
        } catch (Exception e) {
            assertTrue(StringUtils.containsIgnoreCase(e.getMessage(), "invalid citation type"));
        }
    }

    private Row getProteomeRowWithInvalidCitationType() {
        List<Object> entryValues = new LinkedList<>();
        entryValues.add(UP_ID_VAL);
        entryValues.add(TAXONOMY_VAL);
        entryValues.add(false);
        entryValues.add(true);
        entryValues.add(new Date(EPOCH_MILLI_NOW));
        entryValues.add(
                (Seq)
                        JavaConverters.asScalaIteratorConverter(Collections.emptyIterator())
                                .asScala()
                                .toSeq());
        entryValues.add(getReferenceSeqWithInvalidType());
        return new GenericRowWithSchema(entryValues.toArray(), getProteomeMinimalXMLSchema());
    }

    private Seq getReferenceSeqWithInvalidType() {
        List<Object> referenceSeq = new ArrayList<>();
        referenceSeq.add(getReferenceRow("invalid", List.of(), List.of(), List.of()));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(referenceSeq.iterator()).asScala().toSeq();
    }

    private ProteomeEntry getExpectedMinimalResult() {
        return new ProteomeEntryBuilder()
                .proteomeId(UP_ID_VAL)
                .taxonomy(new TaxonomyBuilder().taxonId(TAXONOMY_VAL).build())
                .modified(
                        Instant.ofEpochMilli(EPOCH_MILLI_NOW)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate())
                .proteomeType(ProteomeType.REPRESENTATIVE)
                .citationsSet(List.of())
                .build();
    }

    private Row getMinimalProteomeRow() {
        List<Object> entryValues = new LinkedList<>();
        entryValues.add(UP_ID_VAL);
        entryValues.add(TAXONOMY_VAL);
        entryValues.add(false);
        entryValues.add(true);
        entryValues.add(new Date(EPOCH_MILLI_NOW));
        entryValues.add(
                (Seq)
                        JavaConverters.asScalaIteratorConverter(Collections.emptyIterator())
                                .asScala()
                                .toSeq());
        return new GenericRowWithSchema(entryValues.toArray(), getProteomeMinimalXMLSchema());
    }

    private StructType getProteomeMinimalXMLSchema() {
        StructType structType = new StructType();
        structType = structType.add(UPID, DataTypes.StringType, false);
        structType = structType.add(TAXONOMY, DataTypes.LongType, false);
        structType = structType.add(IS_REFERENCE_PROTEOME, DataTypes.BooleanType, false);
        structType = structType.add(IS_REPRESENTATIVE_PROTEOME, DataTypes.BooleanType, false);
        structType = structType.add(MODIFIED, DataTypes.DateType, false);
        structType =
                structType.add(COMPONENT, DataTypes.createArrayType(getComponentSchema()), false);
        return structType;
    }

    private ProteomeEntry getExpectedFullResult() {
        return new ProteomeEntryBuilder()
                .proteinCount(Integer.valueOf(PROTEIN_COUNT_VALUE))
                .proteomeId(UP_ID_VAL)
                .taxonomy(new TaxonomyBuilder().taxonId(TAXONOMY_VAL).build())
                .proteomeType(ProteomeType.EXCLUDED)
                .modified(
                        Instant.ofEpochMilli(EPOCH_MILLI_NOW)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate())
                .strain(STRAIN_VAL)
                .description(DESCRIPTION_VAL)
                .isolate(ISOLATE_VAL)
                .redundantTo(new ProteomeIdBuilder(REDUNDANT_TO_VAL).build())
                .panproteome(new ProteomeIdBuilder(PANPROTEOME_VAL).build())
                .annotationScore((int) ANNOTATION_SCORE_SCORE_VAL)
                .genomeAnnotation(
                        new GenomeAnnotationBuilder()
                                .source(GENOME_ANNOTATION_SOURCE_VAL)
                                .url(GENOME_ANNOTATION_URL_VAL)
                                .build())
                .genomeAssembly(
                        new GenomeAssemblyBuilder()
                                .assemblyId(GENOME_ASSEMBLY_ID_VAL)
                                .source(GenomeAssemblySource.ENA)
                                .genomeAssemblyUrl(GENOME_ASSEMBLY_URL_VAL)
                                .level(GenomeAssemblyLevel.FULL)
                                .build())
                .componentsSet(
                        List.of(
                                new ComponentBuilder()
                                        .name(COMPONENT_NAME_0)
                                        .proteinCount((int) COMPONENT_PROTEIN_COUNT_0)
                                        .description(COMPONENT_DESCRIPTION_0)
                                        .genomeAnnotation(
                                                new GenomeAnnotationBuilder()
                                                        .source(COMPONENT_SOURCE_0)
                                                        .build())
                                        .proteomeCrossReferencesAdd(
                                                new CrossReferenceBuilder<ProteomeDatabase>()
                                                        .database(ProteomeDatabase.GENOME_ACCESSION)
                                                        .id("CP001147")
                                                        .build())
                                        .proteomeCrossReferencesAdd(
                                                new CrossReferenceBuilder<ProteomeDatabase>()
                                                        .database(ProteomeDatabase.BIOSAMPLE)
                                                        .id("SAMN02603929")
                                                        .build())
                                        .build(),
                                new ComponentBuilder()
                                        .name(COMPONENT_NAME_1)
                                        .proteinCount((int) COMPONENT_PROTEIN_COUNT_1)
                                        .description(COMPONENT_DESCRIPTION_1)
                                        .genomeAnnotation(
                                                new GenomeAnnotationBuilder()
                                                        .source(COMPONENT_SOURCE_1)
                                                        .build())
                                        .proteomeCrossReferencesAdd(
                                                new CrossReferenceBuilder<ProteomeDatabase>()
                                                        .database(ProteomeDatabase.GENOME_ACCESSION)
                                                        .id("M96262")
                                                        .build())
                                        .proteomeCrossReferencesAdd(
                                                new CrossReferenceBuilder<ProteomeDatabase>()
                                                        .database(ProteomeDatabase.BIOSAMPLE)
                                                        .id("SAMN02603927")
                                                        .build())
                                        .build()))
                .citationsSet(
                        List.of(
                                new JournalArticleBuilder()
                                        .journalName(CITATION_NAME_0)
                                        .firstPage(CITATION_FIRST_0)
                                        .lastPage(CITATION_LAST_0)
                                        .volume(CITATION_VOLUME_0)
                                        .title(CITATION_TITLE_0)
                                        .publicationDate(CITATION_DATE_0)
                                        .authorsSet(
                                                new LinkedList<>(
                                                        List.of(AUTHOR_NAME_0_0, AUTHOR_NAME_1_0)))
                                        .authoringGroupsSet(
                                                new LinkedList<>(
                                                        List.of(
                                                                CONSORTIUM_NAME_0_0,
                                                                CONSORTIUM_NAME_0_1)))
                                        .citationCrossReferencesSet(
                                                List.of(
                                                        new CrossReferenceBuilder<
                                                                        CitationDatabase>()
                                                                .database(CitationDatabase.PUBMED)
                                                                .id(DB_REF_VALUE_0_0)
                                                                .build(),
                                                        new CrossReferenceBuilder<
                                                                        CitationDatabase>()
                                                                .database(CitationDatabase.DOI)
                                                                .id(DB_REF_VALUE_1_0)
                                                                .build()))
                                        .build(),
                                new SubmissionBuilder()
                                        .submittedToDatabase(EMBL_GENBANK_DDBJ)
                                        .title(CITATION_TITLE_1)
                                        .publicationDate("AUG-2008")
                                        .authorsSet(
                                                new LinkedList<>(
                                                        List.of(AUTHOR_NAME_0_1, AUTHOR_NAME_1_1)))
                                        .authoringGroupsSet(
                                                new LinkedList<>(
                                                        List.of(
                                                                CONSORTIUM_NAME_1_0,
                                                                CONSORTIUM_NAME_1_1)))
                                        .citationCrossReferencesSet(
                                                List.of(
                                                        new CrossReferenceBuilder<
                                                                        CitationDatabase>()
                                                                .database(CitationDatabase.DOI)
                                                                .id(DB_REF_VALUE_0_1)
                                                                .build(),
                                                        new CrossReferenceBuilder<
                                                                        CitationDatabase>()
                                                                .database(CitationDatabase.PUBMED)
                                                                .id(DB_REF_VALUE_1_1)
                                                                .build()))
                                        .build(),
                                new BookBuilder()
                                        .firstPage(CITATION_FIRST_2)
                                        .lastPage(CITATION_LAST_2)
                                        .volume(CITATION_VOLUME_2)
                                        .build(),
                                new PatentBuilder().build(),
                                new ThesisBuilder().build(),
                                new ElectronicArticleBuilder().build(),
                                new LiteratureBuilder().build(),
                                new UnpublishedBuilder().build()))
                .exclusionReasonsAdd(ExclusionReason.MISSING_TRNA_GENES)
                .redundantProteomesSet(
                        List.of(
                                new RedundantProteomeBuilder()
                                        .proteomeId(REDUNDANT_PROTEIN_ID_0)
                                        .similarity(
                                                Float.parseFloat(REDUNDANT_PROTEIN_SIMILARITY_0))
                                        .build(),
                                new RedundantProteomeBuilder()
                                        .proteomeId(REDUNDANT_PROTEIN_ID_1)
                                        .similarity(
                                                Float.parseFloat(REDUNDANT_PROTEIN_SIMILARITY_1))
                                        .build()))
                .proteomeCompletenessReport(
                        new ProteomeCompletenessReportBuilder()
                                .cpdReport(
                                        new CPDReportBuilder()
                                                .confidence(1)
                                                .averageCdss(5)
                                                .status(STANDARD)
                                                .build())
                                .build())
                .build();
    }

    private Row getFullProteomeRow() {
        List<Object> entryValues = new LinkedList<>();
        entryValues.add(PROTEIN_COUNT_VALUE);
        entryValues.add(UP_ID_VAL);
        entryValues.add(TAXONOMY_VAL);
        entryValues.add(IS_REFERENCE_PROTEOME_VAL);
        entryValues.add(IS_REPRESENTATIVE_PROTEOME_VAL);
        entryValues.add(new Date(EPOCH_MILLI_NOW));
        entryValues.add(STRAIN_VAL);
        entryValues.add(DESCRIPTION_VAL);
        entryValues.add(ISOLATE_VAL);
        entryValues.add(REDUNDANT_TO_VAL);
        entryValues.add(PANPROTEOME_VAL);
        entryValues.add(getGenomeAnnotationScoreRow());
        entryValues.add(getGenomeAnnotationRow());
        entryValues.add(getGenomeAssemblyRow());
        entryValues.add(getComponentSeq());
        entryValues.add(getScoresSeq());
        entryValues.add(getReferenceSeq());
        entryValues.add(getRedundantProteomes());
        entryValues.add(getExclusion());

        return new GenericRowWithSchema(entryValues.toArray(), getProteomeXMLSchema());
    }

    private WrappedArray getExclusion() {
        List<Object> exclusion = new ArrayList<>();
        exclusion.add(getExclusionReasons());
        Row[] array =
                exclusion.stream()
                        .map(
                                e ->
                                        new GenericRowWithSchema(
                                                exclusion.toArray(), getExclusionSchema()))
                        .toArray(Row[]::new);
        return new WrappedArray.ofRef<>(array);
    }

    private WrappedArray getExclusionReasons() {
        String[] exclusionReasons = new String[] {EXCLUSION_REASON_VAL};
        return new WrappedArray.ofRef<>(exclusionReasons);
    }

    private Seq getRedundantProteomes() {
        List<Object> redundantProteomes = new ArrayList<>();
        redundantProteomes.add(
                getRedundantProteome(REDUNDANT_PROTEIN_ID_0, REDUNDANT_PROTEIN_SIMILARITY_0));
        redundantProteomes.add(
                getRedundantProteome(REDUNDANT_PROTEIN_ID_1, REDUNDANT_PROTEIN_SIMILARITY_1));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(redundantProteomes.iterator())
                        .asScala()
                        .toSeq();
    }

    private Row getRedundantProteome(String name, String similarity) {
        List<Object> redundantProteome = new ArrayList<>();
        redundantProteome.add(name);
        redundantProteome.add(similarity);
        return new GenericRowWithSchema(redundantProteome.toArray(), getRedundantProteomeSchema());
    }

    private Seq getComponentSeq() {
        List<Object> components = new ArrayList<>();
        components.add(
                getComponentRow(
                        COMPONENT_NAME_0,
                        COMPONENT_PROTEIN_COUNT_0,
                        COMPONENT_BIO_SAMPLE_ID_0,
                        COMPONENT_DESCRIPTION_0,
                        COMPONENT_GENOME_ACCESSION_0,
                        COMPONENT_SOURCE_0));
        components.add(
                getComponentRow(
                        COMPONENT_NAME_1,
                        COMPONENT_PROTEIN_COUNT_1,
                        COMPONENT_BIO_SAMPLE_ID_1,
                        COMPONENT_DESCRIPTION_1,
                        COMPONENT_GENOME_ACCESSION_1,
                        COMPONENT_SOURCE_1));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(components.iterator()).asScala().toSeq();
    }

    private Row getComponentRow(
            String name,
            long proteinCount,
            String bioSampleId,
            String description,
            String genomeAccession,
            String source) {
        List<Object> component = new ArrayList<>();
        component.add(name);
        component.add(proteinCount);
        component.add(bioSampleId);
        component.add(description);
        component.add(getGenomeAccessions(genomeAccession));
        component.add(getGenomeAnnotationSourceRow(source));
        return new GenericRowWithSchema(component.toArray(), getComponentSchema());
    }

    private WrappedArray getGenomeAccessions(String genomeAccession) {
        String[] exclusionReasons = new String[] {genomeAccession};
        return new WrappedArray.ofRef<>(exclusionReasons);
    }

    private Row getGenomeAnnotationSourceRow(String genomeAnnotationSourceVal) {
        List<Object> genomeAnnotationSource = new ArrayList<>();
        genomeAnnotationSource.add(genomeAnnotationSourceVal);
        return new GenericRowWithSchema(
                genomeAnnotationSource.toArray(), getGenomeAnnotationSourceSchema());
    }

    private Seq getScoresSeq() {
        List<Object> scores = new ArrayList<>();
        scores.add(
                getScoresRow(
                        SCORES_NAME_0,
                        List.of(
                                new Tuple2<>(SCORES_SCORE_0_0, SCORES_VALUE_0_0),
                                new Tuple2<>(SCORES_SCORE_1_0, SCORES_VALUE_1_0),
                                new Tuple2<>(SCORES_SCORE_2_0, SCORES_VALUE_2_0))));
        scores.add(
                getScoresRow(
                        SCORES_NAME_1,
                        List.of(
                                new Tuple2<>(SCORES_SCORE_0_1, SCORES_VALUE_0_1),
                                new Tuple2<>(SCORES_SCORE_1_1, SCORES_VALUE_1_1),
                                new Tuple2<>(SCORES_SCORE_2_1, SCORES_VALUE_2_1))));
        return (Seq) JavaConverters.asScalaIteratorConverter(scores.iterator()).asScala().toSeq();
    }

    private Row getScoresRow(String name, List<Tuple2> properties) {
        List<Object> score = new ArrayList<>();
        score.add(name);
        score.add(getPropertySequence(properties));
        return new GenericRowWithSchema(score.toArray(), getScoresSchema());
    }

    private Seq getPropertySequence(List<Tuple2> properties) {
        List<Object> propertyItems = new ArrayList<>();
        for (Tuple2 property : properties) {
            propertyItems.add(getPropertyRow(property));
        }
        return (Seq)
                JavaConverters.asScalaIteratorConverter(propertyItems.iterator()).asScala().toSeq();
    }

    private Row getPropertyRow(Tuple2 property) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add("_VALUE");
        propertyValues.add(property._1);
        propertyValues.add(property._2);
        return new GenericRowWithSchema(propertyValues.toArray(), getPropertySchema());
    }

    private Seq getReferenceSeq() {
        List<Object> referenceSeq = new ArrayList<>();
        referenceSeq.add(
                getReferenceRow(
                        CITATION_TYPE_0,
                        List.of(
                                CITATION_DATE_0,
                                CITATION_DB_0,
                                CITATION_FIRST_0,
                                CITATION_LAST_0,
                                CITATION_NAME_0,
                                CITATION_VOLUME_0,
                                CITATION_TITLE_0),
                        List.of(
                                "",
                                AUTHOR_NAME_0_0,
                                "",
                                AUTHOR_NAME_1_0,
                                CONSORTIUM_NAME_0_0,
                                CONSORTIUM_NAME_0_1),
                        List.of(
                                DB_REF_VALUE_0_0,
                                DB_REF_NAME_0_0,
                                DB_REF_VALUE_1_0,
                                DB_REF_NAME_1_0)));
        referenceSeq.add(
                getReferenceRow(
                        CITATION_TYPE_1,
                        List.of(
                                CITATION_DATE_1,
                                CITATION_DB_1,
                                CITATION_FIRST_1,
                                CITATION_LAST_1,
                                CITATION_NAME_1,
                                CITATION_VOLUME_1,
                                CITATION_TITLE_1),
                        List.of(
                                "",
                                AUTHOR_NAME_0_1,
                                "",
                                AUTHOR_NAME_1_1,
                                CONSORTIUM_NAME_1_0,
                                CONSORTIUM_NAME_1_1),
                        List.of(
                                DB_REF_VALUE_0_1,
                                DB_REF_NAME_0_1,
                                DB_REF_VALUE_1_1,
                                DB_REF_NAME_1_1)));
        referenceSeq.add(
                getReferenceBookRowMinimal(
                        List.of(CITATION_FIRST_2, CITATION_LAST_2, CITATION_VOLUME_2)));
        referenceSeq.add(getReferenceRowMinimal(CITATION_TYPE_3));
        referenceSeq.add(getReferenceRowMinimal(CITATION_TYPE_4));
        referenceSeq.add(getReferenceRowMinimal(CITATION_TYPE_5));
        referenceSeq.add(getReferenceRowMinimal(CITATION_TYPE_6));
        referenceSeq.add(getReferenceRowMinimal(CITATION_TYPE_7));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(referenceSeq.iterator()).asScala().toSeq();
    }

    private Row getReferenceRowMinimal(String type) {
        List<Object> referenceMinimal = new ArrayList<>();
        referenceMinimal.add(getCitationRowMinimal(type));
        return new GenericRowWithSchema(referenceMinimal.toArray(), getReferenceSchemaMinimal());
    }

    private Row getCitationRowMinimal(String type) {
        List<Object> citationMinimal = new ArrayList<>();
        citationMinimal.add(type);
        return new GenericRowWithSchema(citationMinimal.toArray(), getCitationSchemaMinimal());
    }

    private StructType getReferenceSchemaMinimal() {
        StructType referenceMinimal = new StructType();
        referenceMinimal = referenceMinimal.add(CITATION, getCitationSchemaMinimal(), false);
        return referenceMinimal;
    }

    private static StructType getCitationSchemaMinimal() {
        StructType citationMinimal = new StructType();
        citationMinimal = citationMinimal.add(TYPE, DataTypes.StringType, false);
        return citationMinimal;
    }

    private Row getReferenceBookRowMinimal(List<String> otherProperties) {
        List<Object> referenceBookMinimal = new ArrayList<>();
        referenceBookMinimal.add(getCitationBookRowMinimal(otherProperties));
        return new GenericRowWithSchema(
                referenceBookMinimal.toArray(), getReferenceBookSchemaMinimal());
    }

    private Row getCitationBookRowMinimal(List<String> otherProperties) {
        List<Object> citationBookMinimal = new ArrayList<>();
        citationBookMinimal.add(DatasetProteomeEntryConverterTest.CITATION_TYPE_2);
        citationBookMinimal.add(otherProperties.get(0));
        citationBookMinimal.add(otherProperties.get(1));
        citationBookMinimal.add(otherProperties.get(2));
        return new GenericRowWithSchema(
                citationBookMinimal.toArray(), getCitationBookSchemaMinimal());
    }

    private StructType getReferenceBookSchemaMinimal() {
        StructType referenceBookMinimal = new StructType();
        referenceBookMinimal =
                referenceBookMinimal.add(CITATION, getCitationBookSchemaMinimal(), false);
        return referenceBookMinimal;
    }

    private static StructType getCitationBookSchemaMinimal() {
        StructType citationBookMiniaml = new StructType();
        citationBookMiniaml = citationBookMiniaml.add(TYPE, DataTypes.StringType, false);
        citationBookMiniaml = citationBookMiniaml.add(FIRST, DataTypes.StringType, true);
        citationBookMiniaml = citationBookMiniaml.add(LAST, DataTypes.StringType, true);
        citationBookMiniaml = citationBookMiniaml.add(VOLUME, DataTypes.StringType, true);
        return citationBookMiniaml;
    }

    private Row getReferenceRow(
            String type,
            List<String> citationProperties,
            List<String> authorProperties,
            List<String> dbReferenceProperties) {
        List<Object> referenceSeq = new ArrayList<>();
        referenceSeq.add(
                getCitationRow(type, citationProperties, authorProperties, dbReferenceProperties));
        return new GenericRowWithSchema(referenceSeq.toArray(), getReferenceSchema());
    }

    private Row getCitationRow(
            String type,
            List<String> citationProperties,
            List<String> authorProperties,
            List<String> dbReferenceProperties) {
        List<Object> citation = new ArrayList<>();
        citation.add(type);
        if (!citationProperties.isEmpty()) {
            citation.add(citationProperties.get(0));
            citation.add(citationProperties.get(1));
            citation.add(citationProperties.get(2));
            citation.add(citationProperties.get(3));
            citation.add(citationProperties.get(4));
            citation.add(citationProperties.get(5));
            citation.add(citationProperties.get(6));
        }
        if (!authorProperties.isEmpty()) {
            citation.add(getAuthorListRow(authorProperties));
        }
        if (!dbReferenceProperties.isEmpty()) {
            citation.add(geDbReferenceSchemaSequence(dbReferenceProperties));
        }

        return new GenericRowWithSchema(citation.toArray(), getCitationSchema());
    }

    private Row getAuthorListRow(List<String> authorProperties) {
        List<Object> authorSeq = new ArrayList<>();
        authorSeq.add(getPersonSequence(authorProperties));
        authorSeq.add(getConsortiumSequence(authorProperties));
        return new GenericRowWithSchema(authorSeq.toArray(), getAuthorListScheme());
    }

    private Seq getConsortiumSequence(List<String> personProperties) {
        List<Object> personSeq = new ArrayList<>();
        personSeq.add(getConsortiumRow("", personProperties.get(4)));
        personSeq.add(getPersonRow("", personProperties.get(5)));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(personSeq.iterator()).asScala().toSeq();
    }

    private Seq getPersonSequence(List<String> personProperties) {
        List<Object> personSeq = new ArrayList<>();
        personSeq.add(getPersonRow(personProperties.get(0), personProperties.get(1)));
        personSeq.add(getPersonRow(personProperties.get(2), personProperties.get(3)));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(personSeq.iterator()).asScala().toSeq();
    }

    private Row getPersonRow(String value, String name) {
        List<Object> person = new ArrayList<>();
        person.add(value);
        person.add(name);
        return new GenericRowWithSchema(person.toArray(), getPersonScheme());
    }

    private Row getConsortiumRow(String value, String name) {
        List<Object> consortium = new ArrayList<>();
        consortium.add(value);
        consortium.add(name);
        return new GenericRowWithSchema(consortium.toArray(), getConsortiumSchema());
    }

    private Seq geDbReferenceSchemaSequence(List<String> dbReferenceProperties) {
        List<Object> dbReferenceSeq = new ArrayList<>();
        dbReferenceSeq.add(
                getDbReferenceRow(dbReferenceProperties.get(0), dbReferenceProperties.get(1)));
        dbReferenceSeq.add(
                getDbReferenceRow(dbReferenceProperties.get(2), dbReferenceProperties.get(3)));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(dbReferenceSeq.iterator())
                        .asScala()
                        .toSeq();
    }

    private Object getDbReferenceRow(String id, String type) {
        List<Object> dbReference = new ArrayList<>();
        dbReference.add("_VALUE");
        dbReference.add(id);
        dbReference.add(type);
        return new GenericRowWithSchema(dbReference.toArray(), getDbReferenceScheme());
    }

    private Row getGenomeAssemblyRow() {
        List<Object> genomeAssembly = new ArrayList<>();
        genomeAssembly.add(GENOME_ASSEMBLY_ID_VAL);
        genomeAssembly.add(GENOME_ASSEMBLY_URL_VAL);
        genomeAssembly.add(GENOME_ASSEMBLY_SOURCE_VAL);
        genomeAssembly.add(GENOME_ASSEMBLY_REPRESENTATION_VAL);

        return new GenericRowWithSchema(genomeAssembly.toArray(), getGenomeAssemblySchema());
    }

    private Row getGenomeAnnotationScoreRow() {
        List<Object> genomeAnnotationScore = new ArrayList<>();
        genomeAnnotationScore.add(ANNOTATION_SCORE_VAL);
        genomeAnnotationScore.add(ANNOTATION_SCORE_SCORE_VAL);

        return new GenericRowWithSchema(
                genomeAnnotationScore.toArray(), getAnnotationScoreSchema());
    }

    private Row getGenomeAnnotationRow() {
        List<Object> genomeAnnotation = new ArrayList<>();
        genomeAnnotation.add(GENOME_ANNOTATION_SOURCE_VAL);
        genomeAnnotation.add(GENOME_ANNOTATION_URL_VAL);

        return new GenericRowWithSchema(genomeAnnotation.toArray(), getGenomeAnnotationSchema());
    }
}
