package org.uniprot.store.spark.indexer.proteome.converter;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.JournalArticleBuilder;
import org.uniprot.core.citation.impl.SubmissionBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.proteome.GenomeAssemblyLevel;
import org.uniprot.core.proteome.GenomeAssemblySource;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeType;
import org.uniprot.core.proteome.impl.ComponentBuilder;
import org.uniprot.core.proteome.impl.GenomeAnnotationBuilder;
import org.uniprot.core.proteome.impl.GenomeAssemblyBuilder;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.store.spark.indexer.common.util.RowUtils;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.uniprot.core.citation.SubmissionDatabase.EMBL_GENBANK_DDBJ;
import static org.uniprot.store.spark.indexer.proteome.ProteomeRDDReader.*;

class DatasetProteomeEntryConverterTest {

    private static final String UP_ID = "UP000000718";
    private static final int TAXONOMY = 289376;
    private static final boolean IS_REFERENCE_PROTEOME = true;
    private static final boolean IS_REPRESENTATIVE_PROTEOME = false;
    private static final String MODIFIED = "2020-10-17";
    private static final String STRAIN = "ATCC 51303 / DSM 11347 / YP87<";
    private static final String DESCRIPTION = "Thermodesulfovibrio yellowstonii (strain ATCC 51303 / DSM 11347 / YP87) is a thermophilic sulfate-reducing bacterium isolated from a thermal vent in Yellowstone Lake in Wyoming, USA. It has the ability to use sulfate, thiosulfate, and sulfite as terminal electron acceptors. Pyruvate can support fermentative growth";
    private static final String GENOME_ANNOTATION_SOURCE = "ENA/EMBL";
    private static final String GENOME_ASSEMBLY_SOURCE = "ENA/EMBL";
    private static final String GENOME_ANNOTATION_URL = "https://www.ebi.ac.uk/ena/browser/view/GCA_000020985.1";
    private static final String GENOME_ASSEMBLY_URL = "https://www.ebi.ac.uk/ena/browser/view/GCA_000020985.1";
    private static final String ANNOTATION_SCORE_VALUE = "Score";
    private static final long ANNOTATION_SCORE_SCORE = 1267L;
    private static final String GENOME_ASSEMBLY_ID = "GCA_000020985.1";
    private static final String GENOME_ASSEMBLY_REPRESENTATION = "full";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String COMPONENT_NAME_0 = "Chromosome";
    private static final long COMPONENT_PROTEIN_COUNT_0 = 10L;
    private static final String COMPONENT_BIO_SAMPLE_ID_0 = "SAMN02603929";
    private static final String COMPONENT_DESCRIPTION_0 = "Thermodesulfovibrio yellowstonii DSM 11347";
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
    private static final String CITATION_TITLE_0 = "Molecular cloning and complete nucleotide sequence of galinsoga mosaic virus genomic RNA.";
    private static final String CITATION_DATE_1 = "2008-08";
    private static final String CITATION_DB_1 = "EMBL/GenBank/DDBJ databases";
    private static final String CITATION_FIRST_1 = "0";
    private static final String CITATION_LAST_1 = "0";
    private static final String CITATION_NAME_1 = "";
    private static final String CITATION_TYPE_1 = "submission";
    private static final String CITATION_VOLUME_1 = "0";
    private static final String CITATION_TITLE_1 = "The complete genome sequence of Thermodesulfovibrio yellowstonii strain ATCC 51303 / DSM 11347 / YP87.";
    private static final String AUTHOR_NAME_0_0 = "Briddon R.W.";
    private static final String AUTHOR_NAME_1_0 = "Heydarnejad J.";
    private static final String AUTHOR_NAME_0_1 = "Khosrowfar F.";
    private static final String AUTHOR_NAME_1_1 = "Massumi H.";
    private static final String CONSORTIUM_VALUE_0_0 = "25635016";
    private static final String CONSORTIUM_VALUE_0_1 = "10.1016/j.virusres.2010.05.016";
    private static final String CONSORTIUM_VALUE_1_0 = "10.1006/viro.1993.1008";
    private static final String CONSORTIUM_VALUE_1_1 = "20566344";
    private static final String CONSORTIUM_NAME_0_0 = "PubMed";
    private static final String CONSORTIUM_NAME_1_0 = "DOI";
    private static final String CONSORTIUM_NAME_1_1 = "PubMed";
    private static final String CONSORTIUM_NAME_0_1 = "DOI";
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
    private final DatasetProteomeEntryConverter proteomeEntryConverter = new DatasetProteomeEntryConverter();

    @Test
    void fullProteomeEntry() throws Exception {
        Row row = getFullProteomeRow();

        ProteomeEntry result = proteomeEntryConverter.call(row);

        assertThat(result, samePropertyValuesAs(getExpectedResult()));
    }

    @Test
    void requiredOnly() throws Exception {
        ProteomeEntry result = proteomeEntryConverter.call(new GenericRowWithSchema(new Object[0], new StructType()));

        assertThat(result, samePropertyValuesAs(new ProteomeEntryBuilder().build()));
    }

    private ProteomeEntry getExpectedResult() {
        return new ProteomeEntryBuilder().proteomeId(UP_ID)
                .taxonomy(new TaxonomyBuilder().taxonId(TAXONOMY).build())
                .proteomeType(ProteomeType.REFERENCE)
                .modified(LocalDate.parse(MODIFIED, formatter))
                .strain(STRAIN)
                .description(DESCRIPTION)
                .annotationScore((int) ANNOTATION_SCORE_SCORE)
                .genomeAnnotation(new GenomeAnnotationBuilder().source(GENOME_ANNOTATION_SOURCE).url(GENOME_ANNOTATION_URL).build())
                .genomeAssembly(new GenomeAssemblyBuilder().assemblyId(GENOME_ASSEMBLY_ID).source(GenomeAssemblySource.ENA).genomeAssemblyUrl(GENOME_ASSEMBLY_URL).level(GenomeAssemblyLevel.FULL).build())
                .componentsSet(List.of(
                        new ComponentBuilder().name(COMPONENT_NAME_0).proteinCount((int) COMPONENT_PROTEIN_COUNT_0).description(COMPONENT_DESCRIPTION_0).genomeAnnotation(new GenomeAnnotationBuilder().source(COMPONENT_SOURCE_0).build()).build(),
                        new ComponentBuilder().name(COMPONENT_NAME_1).proteinCount((int) COMPONENT_PROTEIN_COUNT_1).description(COMPONENT_DESCRIPTION_1).genomeAnnotation(new GenomeAnnotationBuilder().source(COMPONENT_SOURCE_1).build()).build()
                ))
                .citationsSet(List.of(
                        new JournalArticleBuilder().journalName(CITATION_NAME_0).firstPage(CITATION_FIRST_0).lastPage(CITATION_LAST_0).volume(CITATION_VOLUME_0).title(CITATION_TITLE_0).publicationDate(CITATION_DATE_0)
                                .authorsSet(new LinkedList<>(List.of(AUTHOR_NAME_0_0, AUTHOR_NAME_1_0)))
                                .citationCrossReferencesSet(List.of(new CrossReferenceBuilder<CitationDatabase>().database(CitationDatabase.PUBMED).id(CONSORTIUM_VALUE_0_0).build(), new CrossReferenceBuilder<CitationDatabase>().database(CitationDatabase.DOI).id(CONSORTIUM_VALUE_1_0).build()))
                                .build(),
                        new SubmissionBuilder().submittedToDatabase(EMBL_GENBANK_DDBJ).title(CITATION_TITLE_1).publicationDate(CITATION_DATE_1)
                                .authorsSet(new LinkedList<>(List.of(AUTHOR_NAME_0_1, AUTHOR_NAME_1_1)))
                                .citationCrossReferencesSet(List.of(new CrossReferenceBuilder<CitationDatabase>().database(CitationDatabase.DOI).id(CONSORTIUM_VALUE_0_1).build(), new CrossReferenceBuilder<CitationDatabase>().database(CitationDatabase.PUBMED).id(CONSORTIUM_VALUE_1_1).build()))
                                .build()
                ))
                .build();
    }

    private Row getFullProteomeRow() {
        List<Object> entryValues = new LinkedList<>();
        entryValues.add(UP_ID);
        entryValues.add(TAXONOMY);
        entryValues.add(IS_REFERENCE_PROTEOME);
        entryValues.add(IS_REPRESENTATIVE_PROTEOME);
        entryValues.add(MODIFIED);
        entryValues.add(STRAIN);
        entryValues.add(DESCRIPTION);
        entryValues.add(getGenomeAnnotationScoreRow());
        entryValues.add(getGenomeAnnotationRow());
        entryValues.add(getGenomeAssemblyRow());
        entryValues.add(getComponentSeq());
        entryValues.add(getScoresSeq());
        entryValues.add(getReferenceSeq());

        return new GenericRowWithSchema(entryValues.toArray(), geProteomeXMLSchema());
    }

    private Seq getComponentSeq() {
        List<Object> properties = new ArrayList<>();
        properties.add(getComponentRow(COMPONENT_NAME_0, COMPONENT_PROTEIN_COUNT_0, COMPONENT_BIO_SAMPLE_ID_0, COMPONENT_DESCRIPTION_0,
                COMPONENT_GENOME_ACCESSION_0, COMPONENT_SOURCE_0));
        properties.add(getComponentRow(COMPONENT_NAME_1, COMPONENT_PROTEIN_COUNT_1, COMPONENT_BIO_SAMPLE_ID_1, COMPONENT_DESCRIPTION_1,
                COMPONENT_GENOME_ACCESSION_1, COMPONENT_SOURCE_1));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Row getComponentRow(String name, long proteinCount, String bioSampleId, String description, String genomeAccession, String source) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add(name);
        propertyValues.add(proteinCount);
        propertyValues.add(bioSampleId);
        propertyValues.add(description);
        propertyValues.add(genomeAccession);
        propertyValues.add(getGenomeAnnotationSourceRow(source));
        return new GenericRowWithSchema(propertyValues.toArray(), getComponentSchema());
    }

    private Row getGenomeAnnotationSourceRow(String genomeAnnotationSource) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add(genomeAnnotationSource);
        return new GenericRowWithSchema(propertyValues.toArray(), getGenomeAnnotationSourceSchema());
    }

    private Seq getScoresSeq() {
        List<Object> properties = new ArrayList<>();
        properties.add(getScoresRow(SCORES_NAME_0, List.of(new Tuple2<>(SCORES_SCORE_0_0, SCORES_VALUE_0_0), new Tuple2<>(SCORES_SCORE_1_0, SCORES_VALUE_1_0), new Tuple2<>(SCORES_SCORE_2_0, SCORES_VALUE_2_0))));
        properties.add(getScoresRow(SCORES_NAME_1, List.of(new Tuple2<>(SCORES_SCORE_0_1, SCORES_VALUE_0_1), new Tuple2<>(SCORES_SCORE_1_1, SCORES_VALUE_1_1), new Tuple2<>(SCORES_SCORE_2_1, SCORES_VALUE_2_1))));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Row getScoresRow(String name, List<Tuple2> properties) {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add(name);
        entryValues.add(getPropertySequence(properties));
        return new GenericRowWithSchema(entryValues.toArray(), getScoresSchema());
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
        return new GenericRowWithSchema(propertyValues.toArray(), RowUtils.getPropertySchema());
    }

    private Seq getReferenceSeq() {
        List<Object> properties = new ArrayList<>();
        properties.add(getReferenceRow(List.of(CITATION_DATE_0, CITATION_DB_0, CITATION_FIRST_0, CITATION_LAST_0, CITATION_NAME_0, CITATION_TYPE_0, CITATION_VOLUME_0, CITATION_TITLE_0),
                List.of("", AUTHOR_NAME_0_0, "", AUTHOR_NAME_1_0, "", ""),
                List.of(CONSORTIUM_VALUE_0_0, CONSORTIUM_NAME_0_0, CONSORTIUM_VALUE_1_0, CONSORTIUM_NAME_1_0)));
        properties.add(getReferenceRow(List.of(CITATION_DATE_1, CITATION_DB_1, CITATION_FIRST_1, CITATION_LAST_1, CITATION_NAME_1, CITATION_TYPE_1, CITATION_VOLUME_1, CITATION_TITLE_1),
                List.of("", AUTHOR_NAME_0_1, "", AUTHOR_NAME_1_1, "", ""),
                List.of(CONSORTIUM_VALUE_0_1, CONSORTIUM_NAME_0_1, CONSORTIUM_VALUE_1_1, CONSORTIUM_NAME_1_1)));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Row getReferenceRow(List<String> citationProperties, List<String> authorProperties, List<String> dbReferenceProperties) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add(getCitationRow(citationProperties, authorProperties, dbReferenceProperties));
        return new GenericRowWithSchema(propertyValues.toArray(), getReferenceSchema());
    }

    private Row getCitationRow(List<String> citationProperties, List<String> authorProperties, List<String> dbReferenceProperties) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add(citationProperties.get(0));
        propertyValues.add(citationProperties.get(1));
        propertyValues.add(Long.parseLong(citationProperties.get(2)));
        propertyValues.add(Long.parseLong(citationProperties.get(3)));
        propertyValues.add(citationProperties.get(4));
        propertyValues.add(citationProperties.get(5));
        propertyValues.add(Long.parseLong(citationProperties.get(6)));
        propertyValues.add(citationProperties.get(7));
        propertyValues.add(getAuthorListRow(authorProperties));
        propertyValues.add(geDbReferenceSchemaSequence(dbReferenceProperties));

        return new GenericRowWithSchema(propertyValues.toArray(), getCitationSchema());
    }

    private Row getAuthorListRow(List<String> authorProperties) {
        List<Object> properties = new ArrayList<>();
        properties.add(getPersonSequence(authorProperties));
        properties.add(getConsortiumRow(authorProperties.get(4), authorProperties.get(5)));
        return new GenericRowWithSchema(properties.toArray(), getAuthorScheme());
    }

    private Seq getPersonSequence(List<String> personProperties) {
        List<Object> properties = new ArrayList<>();
        properties.add(getPersonRow(personProperties.get(0), personProperties.get(1)));
        properties.add(getPersonRow(personProperties.get(2), personProperties.get(3)));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Row getPersonRow(String value, String name) {
        List<Object> properties = new ArrayList<>();
        properties.add(value);
        properties.add(name);
        return new GenericRowWithSchema(properties.toArray(), getPersonScheme());
    }

    private Row getConsortiumRow(String value, String name) {
        List<Object> properties = new ArrayList<>();
        properties.add(value);
        properties.add(name);
        return new GenericRowWithSchema(properties.toArray(), getConsortiumSchema());
    }

    private Seq geDbReferenceSchemaSequence(List<String> dbReferenceProperties) {
        List<Object> properties = new ArrayList<>();
        properties.add(getDbReferenceRow(dbReferenceProperties.get(0), dbReferenceProperties.get(1)));
        properties.add(getDbReferenceRow(dbReferenceProperties.get(2), dbReferenceProperties.get(3)));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Object getDbReferenceRow(String id, String type) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add("_VALUE");
        propertyValues.add(id);
        propertyValues.add(type);
        return new GenericRowWithSchema(propertyValues.toArray(), getDbReferenceScheme());
    }

    private Row getGenomeAssemblyRow() {
        List<Object> sequenceValues = new ArrayList<>();
        sequenceValues.add(GENOME_ASSEMBLY_ID);
        sequenceValues.add(GENOME_ASSEMBLY_URL);
        sequenceValues.add(GENOME_ASSEMBLY_SOURCE);
        sequenceValues.add(GENOME_ASSEMBLY_REPRESENTATION);

        return new GenericRowWithSchema(sequenceValues.toArray(), getGenomeAssemblySchema());
    }

    private Row getGenomeAnnotationScoreRow() {
        List<Object> sequenceValues = new ArrayList<>();
        sequenceValues.add(ANNOTATION_SCORE_VALUE);
        sequenceValues.add(ANNOTATION_SCORE_SCORE);

        return new GenericRowWithSchema(sequenceValues.toArray(), getAnnotationScoreSchema());
    }

    private Row getGenomeAnnotationRow() {
        List<Object> sequenceValues = new ArrayList<>();
        sequenceValues.add(GENOME_ANNOTATION_SOURCE);
        sequenceValues.add(GENOME_ANNOTATION_URL);

        return new GenericRowWithSchema(sequenceValues.toArray(), getGenomeAnnotationSchema());
    }
}