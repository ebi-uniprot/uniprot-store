package org.uniprot.store.spark.indexer.proteome.converter;

import lombok.Data;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.CitationType;
import org.uniprot.core.citation.SubmissionDatabase;
import org.uniprot.core.citation.impl.*;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.proteome.*;
import org.uniprot.core.proteome.impl.*;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.core.util.Utils;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.uniprot.core.util.Utils.notNullNotEmpty;
import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.proteome.ProteomeXMLSchemaProvider.*;

/**
 * Converts XML {@link Row} instances to {@link ProteomeEntry} instances.
 *
 * @author sahmad
 * @created 21/08/2020
 */
public class DatasetProteomeEntryConverter implements Function<Row, ProteomeEntry>, Serializable {
    private static final long serialVersionUID = 6017417913038106086L;
    public static final String PROPERTY_TOTAL = "total";
    public static final String PROPERTY_COMPLETED = "completed";
    public static final String PROPERTY_COMPLETED_SINGLE = "completedSingle";
    public static final String PROPERTY_COMPLETED_DUPLICATED = "completedDuplicated";
    public static final String PROPERTY_FRAGMENTED = "fragmented";
    public static final String PROPERTY_MISSING = "missing";
    public static final String PROPERTY_SCORE = "score";
    public static final String PROPERTY_LINEAGE = "lineage";
    public static final String PROPERTY_AVERAGE_CDS = "averageCds";
    public static final String PROPERTY_STATUS = "status";
    public static final String PROPERTY_CONFIDENCE = "confidence";
    public static final String PROPERTY_PROTEOME_COUNT = "proteomeCount";
    public static final String PROPERTY_STD_CDSS = "stdCdss";
    public static final String BUSCO = "busco";
    public static final String CPD = "cpd";

    @Override
    public ProteomeEntry call(Row row) throws Exception {
        ProteomeEntryBuilder builder = new ProteomeEntryBuilder();
        if (hasFieldName(PROTEIN_COUNT, row)) {
            builder.proteinCount(row.getInt(row.fieldIndex(PROTEIN_COUNT)));
        }
        builder.proteomeId(row.getString(row.fieldIndex(UPID)));
        Taxonomy taxonomy =
                new TaxonomyBuilder().taxonId(row.getLong(row.fieldIndex(TAXONOMY))).build();
        builder.taxonomy(taxonomy);
        if (hasFieldName(STRAIN, row)) {
            builder.strain(row.getString(row.fieldIndex(STRAIN)));
        }
        if (hasFieldName(DESCRIPTION, row)) {
            builder.description(row.getString(row.fieldIndex(DESCRIPTION)));
        }
        if (hasFieldName(ISOLATE, row)) {
            builder.isolate(row.getString(row.fieldIndex(ISOLATE)));
        }
        boolean isRedundant = false;
        if (hasFieldName(REDUNDANT_TO, row)) {
            isRedundant = true;
            builder.redundantTo(
                    new ProteomeIdBuilder((row.getString(row.fieldIndex(REDUNDANT_TO)))).build());
        }
        if (hasFieldName(PANPROTEOME, row)) {
            builder.panproteome(
                    new ProteomeIdBuilder((row.getString(row.fieldIndex(PANPROTEOME)))).build());
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'Z'");
        String dateString = row.getDate(row.fieldIndex(MODIFIED)).toString();
        boolean dateEndsWithZ = dateString.endsWith("Z");
        builder.modified(LocalDate.parse(dateEndsWithZ ? dateString : dateString + "Z", formatter));
        if (hasFieldName(GENOME_ANNOTATION, row)) {
            GenomeAnnotation genomeAnnotation =
                    getGenomeAnnotation((Row) row.get(row.fieldIndex(GENOME_ANNOTATION)));
            builder.genomeAnnotation(genomeAnnotation);
        }
        if (hasFieldName(GENOME_ASSEMBLY, row)) {
            GenomeAssembly genomeAssembly =
                    getGenomeAssembly((Row) row.get(row.fieldIndex(GENOME_ASSEMBLY)));
            builder.genomeAssembly(genomeAssembly);
        }
        if (hasFieldName(ANNOTATION_SCORE, row)) {
            Integer annotationScore =
                    getAnnotationScore((Row) row.get(row.fieldIndex(ANNOTATION_SCORE)));
            builder.annotationScore(annotationScore);
        }
        List<Row> componentRows = row.getList(row.fieldIndex(COMPONENT));
        builder.componentsSet(
                componentRows.stream().map(this::getComponent).collect(Collectors.toList()));
        if (hasFieldName(REFERENCE, row)) {
            List<Row> referenceRows = row.getList(row.fieldIndex(REFERENCE));
            builder.citationsSet(
                    referenceRows.stream().map(this::getCitation).collect(Collectors.toList()));
        }
        if (hasFieldName(REDUNDANT_PROTEOME, row)) {
            List<Row> redundantProteomeRows = row.getList(row.fieldIndex(REDUNDANT_PROTEOME));
            builder.redundantProteomesSet(
                    redundantProteomeRows.stream()
                            .map(this::getRedundantProteome)
                            .collect(Collectors.toList()));
        }
        if (hasFieldName(SCORES, row)) {
            List<Row> scoreRows = row.getList(row.fieldIndex(SCORES));
            builder.proteomeCompletenessReport(
                    getCompletenessReport(
                            scoreRows.stream().map(this::getScore).collect(Collectors.toList())));
        }
        List<ExclusionReason> exclusionReasons = List.of();
        if (hasFieldName(EXCLUDED, row)) {
            Row[] excludedReasonRows =
                    (Row[]) ((WrappedArray) row.get(row.fieldIndex(EXCLUDED))).array();
            exclusionReasons =
                    Arrays.stream(excludedReasonRows)
                            .map(this::getExclusionReasons)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
            builder.exclusionReasonsSet(exclusionReasons);
        }
        boolean isReference = row.getBoolean(row.fieldIndex(IS_REFERENCE_PROTEOME));
        boolean isRepresentative = row.getBoolean(row.fieldIndex(IS_REPRESENTATIVE_PROTEOME));
        builder.proteomeType(
                getProteomeType(
                        isReference, isRepresentative, !exclusionReasons.isEmpty(), isRedundant));

        return builder.build();
    }

    private Score getScore(Row row) {
        String name = row.getString(row.fieldIndex(NAME));
        List<Property> properties = List.of();
        if (hasFieldName(PROPERTY, row)) {
            List<Row> list = row.getList(row.fieldIndex(PROPERTY));
            properties = list.stream().map(this::getProperty).collect(Collectors.toList());
        }
        return new Score(name, properties);
    }

    private Property getProperty(Row row) {
        return new Property(
                row.getString(row.fieldIndex(NAME)), row.getString(row.fieldIndex(VALUE_LOWER)));
    }

    private ProteomeCompletenessReport getCompletenessReport(List<Score> scores) {
        ProteomeCompletenessReport result = null;
        if (notNullNotEmpty(scores)) {
            ProteomeCompletenessReportBuilder builder = new ProteomeCompletenessReportBuilder();
            Optional<BuscoReport> buscoReport =
                    scores.stream()
                            .filter(score -> score.getName().equalsIgnoreCase(BUSCO))
                            .map(this::getBuscoReport)
                            .findFirst();
            builder.buscoReport(buscoReport.orElse(null));

            Optional<CPDReport> cpdReport =
                    scores.stream()
                            .filter(score -> score.getName().equalsIgnoreCase(CPD))
                            .map(this::getCPDReport)
                            .findFirst();
            builder.cpdReport(cpdReport.orElse(null));
            result = builder.build();
        }
        return result;
    }

    private BuscoReport getBuscoReport(Score score) {
        BuscoReport result = null;
        if (Utils.notNull(score)) {
            List<Property> properties = score.getProperties();
            BuscoReportBuilder builder = new BuscoReportBuilder();
            for (Property property : properties) {
                switch (property.getName()) {
                    case PROPERTY_COMPLETED:
                        builder.complete(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_COMPLETED_SINGLE:
                        builder.completeSingle(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_COMPLETED_DUPLICATED:
                        builder.completeDuplicated(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_TOTAL:
                        builder.total(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_MISSING:
                        builder.missing(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_FRAGMENTED:
                        builder.fragmented(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_LINEAGE:
                        builder.lineageDb(property.getValue());
                        break;
                    case PROPERTY_SCORE:
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unknown BUSCO property ScorePropertyType.getName: "
                                        + property.getName());
                }
            }
            result = builder.build();
        }
        return result;
    }

    private CPDReport getCPDReport(Score score) {
        CPDReport result = null;
        if (Utils.notNull(score)) {
            CPDReportBuilder builder = new CPDReportBuilder();
            List<Property> properties = score.getProperties();
            for (Property property : properties) {
                switch (property.getName()) {
                    case PROPERTY_AVERAGE_CDS:
                        builder.averageCdss(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_CONFIDENCE:
                        builder.confidence(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_PROTEOME_COUNT:
                        builder.proteomeCount(Integer.parseInt(property.getValue()));
                        break;
                    case PROPERTY_STD_CDSS:
                        BigDecimal stdCdss = new BigDecimal(property.getValue());
                        stdCdss = stdCdss.setScale(2, RoundingMode.HALF_DOWN);
                        builder.stdCdss(stdCdss.doubleValue());
                        break;
                    case PROPERTY_STATUS:
                        builder.status(CPDStatus.fromValue(property.getValue()));
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unknown CPDReport property ScorePropertyType.getName: "
                                        + property.getName());
                }
            }
            result = builder.build();
        }
        return result;
    }

    private static ProteomeType getProteomeType(
            boolean isReference,
            boolean isRepresentative,
            boolean hasExclusionReason,
            boolean hasRedundantProteomes) {
        if (hasExclusionReason) {
            return ProteomeType.EXCLUDED;
        } else if (isReference && isRepresentative) {
            return ProteomeType.REFERENCE_AND_REPRESENTATIVE;
        } else if (isReference) {
            return ProteomeType.REFERENCE;
        } else if (isRepresentative) {
            return ProteomeType.REPRESENTATIVE;
        } else if (hasRedundantProteomes) {
            return ProteomeType.REDUNDANT;
        } else {
            return ProteomeType.NORMAL;
        }
    }

    private RedundantProteome getRedundantProteome(Row row) {
        RedundantProteomeBuilder redundantProteomeBuilder = new RedundantProteomeBuilder();
        redundantProteomeBuilder.proteomeId(row.getString(row.fieldIndex(UPID_ATTRIBUTE)));
        if (hasFieldName(SIMILARITY, row)) {
            redundantProteomeBuilder.similarity(
                    Float.parseFloat(row.getString(row.fieldIndex(SIMILARITY))));
        }
        return redundantProteomeBuilder.build();
    }

    private List<ExclusionReason> getExclusionReasons(Row row) {
        String[] reasons =
                (String[]) ((WrappedArray) row.get(row.fieldIndex(EXCLUSION_REASON))).array();
        return Stream.of(reasons).map(ExclusionReason::typeOf).collect(Collectors.toList());
    }

    private Citation getCitation(Row row) {
        Row citationRow = (Row) row.get(row.fieldIndex(CITATION));
        return getCitationItem(citationRow);
    }

    private Citation getCitationItem(Row row) {
        CitationType citationType = CitationType.typeOf(row.getString(row.fieldIndex(TYPE)));
        switch (citationType) {
            case BOOK:
                return getBook(row);
            case PATENT:
                return getPatent(row);
            case THESIS:
                return getThesis(row);
            case SUBMISSION:
                return getSubmission(row);
            case JOURNAL_ARTICLE:
                return getJournalArticle(row);
            case ELECTRONIC_ARTICLE:
                return getElectronicArticle(row);
            case LITERATURE:
                return getLiterature(row);
            case UNPUBLISHED:
                return getUnpublished(row);
        }
        throw new RuntimeException("Invalid citation type " + citationType);
    }

    private Citation getBook(Row row) {
        BookBuilder bookBuilder = new BookBuilder();
        populateCommon(row, bookBuilder);
        if (hasFieldName(FIRST, row)) {
            bookBuilder.firstPage(row.getString(row.fieldIndex(FIRST)));
        }
        if (hasFieldName(LAST, row)) {
            bookBuilder.lastPage(row.getString(row.fieldIndex(LAST)));
        }
        if (hasFieldName(VOLUME, row)) {
            bookBuilder.volume(row.getString(row.fieldIndex(VOLUME)));
        }
        return bookBuilder.build();
    }

    private Citation getPatent(Row row) {
        PatentBuilder patentBuilder = new PatentBuilder();
        populateCommon(row, patentBuilder);
        return patentBuilder.build();
    }

    private Citation getThesis(Row row) {
        ThesisBuilder thesisBuilder = new ThesisBuilder();
        populateCommon(row, thesisBuilder);
        return thesisBuilder.build();
    }

    private Citation getSubmission(Row row) {
        SubmissionBuilder submissionBuilder = new SubmissionBuilder();
        populateCommon(row, submissionBuilder);
        if (hasFieldName(DB, row)) {
            submissionBuilder.submittedToDatabase(
                    SubmissionDatabase.typeOf(row.getString(row.fieldIndex(DB))));
        }
        return submissionBuilder.build();
    }

    private Citation getJournalArticle(Row row) {
        JournalArticleBuilder journalArticleBuilder = new JournalArticleBuilder();
        populateCommon(row, journalArticleBuilder);
        if (hasFieldName(FIRST, row)) {
            journalArticleBuilder.firstPage(row.getString(row.fieldIndex(FIRST)));
        }
        if (hasFieldName(LAST, row)) {
            journalArticleBuilder.lastPage(row.getString(row.fieldIndex(LAST)));
        }
        if (hasFieldName(VOLUME, row)) {
            journalArticleBuilder.volume(row.getString(row.fieldIndex(VOLUME)));
        }
        if (hasFieldName(NAME, row)) {
            journalArticleBuilder.journalName(row.getString(row.fieldIndex(NAME)));
        }
        return journalArticleBuilder.build();
    }

    private Citation getElectronicArticle(Row row) {
        ElectronicArticleBuilder electronicArticleBuilder = new ElectronicArticleBuilder();
        populateCommon(row, electronicArticleBuilder);
        return electronicArticleBuilder.build();
    }

    private Citation getLiterature(Row row) {
        LiteratureBuilder literatureBuilder = new LiteratureBuilder();
        populateCommon(row, literatureBuilder);
        return literatureBuilder.build();
    }

    private Citation getUnpublished(Row row) {
        UnpublishedBuilder unpublishedBuilder = new UnpublishedBuilder();
        populateCommon(row, unpublishedBuilder);
        return unpublishedBuilder.build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void populateCommon(Row row, AbstractCitationBuilder citationBuilder) {
        if (hasFieldName(AUTHOR_LIST, row)) {
            Row authorsRow = (Row) row.get(row.fieldIndex(AUTHOR_LIST));
            if (hasFieldName(PERSON, authorsRow)) {
                citationBuilder.authorsSet(getAuthors(authorsRow));
            }
            if (hasFieldName(CONSORTIUM, authorsRow)) {
                citationBuilder.authoringGroupsSet(getAuthoringGroups(authorsRow));
            }
        }
        if (hasFieldName(DB_REFERENCE, row)) {
            List<Row> dbReferences = row.getList(row.fieldIndex(DB_REFERENCE));
            citationBuilder.citationCrossReferencesSet(
                    dbReferences.stream().map(this::getCrossRef).collect(Collectors.toList()));
        }
        if (hasFieldName(TITLE, row)) {
            citationBuilder.title((row.getString(row.fieldIndex(TITLE))));
        }
        if (hasFieldName(DATE, row)) {
            citationBuilder.publicationDate((row.getString(row.fieldIndex(DATE))));
        }
    }

    private CrossReference<CitationDatabase> getCrossRef(Row row) {
        return new CrossReferenceBuilder<CitationDatabase>()
                .database(CitationDatabase.typeOf(row.getString(row.fieldIndex(TYPE))))
                .id(row.getString(row.fieldIndex(ID)))
                .build();
    }

    private Collection<String> getAuthors(Row row) {
        List<Row> personList = row.getList(row.fieldIndex(PERSON));
        return personList.stream().map(this::getName).collect(Collectors.toList());
    }

    private List<String> getAuthoringGroups(Row row) {
        List<Row> authoringGroups = row.getList(row.fieldIndex(CONSORTIUM));
        return authoringGroups.stream().map(this::getName).collect(Collectors.toList());
    }

    private String getName(Row row) {
        return row.getString(row.fieldIndex(NAME));
    }

    private Integer getAnnotationScore(Row row) {
        return (int) row.getLong(row.fieldIndex(NORMALIZED_ANNOTATION_SCORE));
    }

    private Component getComponent(Row row) {
        ComponentBuilder componentBuilder = new ComponentBuilder();
        componentBuilder.name(row.getString(row.fieldIndex(NAME)));
        if (hasFieldName(PROTEIN_COUNT, row)) {
            componentBuilder.proteinCount((int) row.getLong(row.fieldIndex(PROTEIN_COUNT)));
        }
        if (hasFieldName(DESCRIPTION, row)) {
            componentBuilder.description(row.getString(row.fieldIndex(DESCRIPTION)));
        }
        if (hasFieldName(GENOME_ANNOTATION, row)) {
            GenomeAnnotation genomeAnnotation =
                    getGenomeAnnotation((Row) row.get(row.fieldIndex(GENOME_ANNOTATION)));
            componentBuilder.genomeAnnotation(genomeAnnotation);
        }
        if (hasFieldName(GENOME_ACCESSION, row)) {
            componentBuilder.proteomeCrossReferencesAdd(
                    new CrossReferenceBuilder<ProteomeDatabase>()
                            .database(ProteomeDatabase.GENOME_ACCESSION)
                            .id(row.getString(row.fieldIndex(GENOME_ACCESSION)))
                            .build());
        }
        if (hasFieldName(BIO_SAMPLE_ID, row)) {
            componentBuilder.proteomeCrossReferencesAdd(
                    new CrossReferenceBuilder<ProteomeDatabase>()
                            .database(ProteomeDatabase.BIOSAMPLE)
                            .id(row.getString(row.fieldIndex(BIO_SAMPLE_ID)))
                            .build());
        }
        return componentBuilder.build();
    }

    private GenomeAssembly getGenomeAssembly(Row row) {
        GenomeAssemblyBuilder genomeAssemblyBuilder = new GenomeAssemblyBuilder();
        if (hasFieldName(GENOME_ASSEMBLY, row)) {
            genomeAssemblyBuilder.assemblyId(row.getString(row.fieldIndex(GENOME_ASSEMBLY)));
        }
        genomeAssemblyBuilder.source(
                GenomeAssemblySource.fromValue(
                        (row.getString(row.fieldIndex(GENOME_ASSEMBLY_SOURCE)))));
        if (hasFieldName(GENOME_ASSEMBLY_URL, row)) {
            genomeAssemblyBuilder.genomeAssemblyUrl(
                    row.getString(row.fieldIndex(GENOME_ASSEMBLY_URL)));
        }
        if (hasFieldName(GENOME_REPRESENTATION, row)) {
            genomeAssemblyBuilder.level(
                    GenomeAssemblyLevel.fromValue(
                            row.getString(row.fieldIndex(GENOME_REPRESENTATION))));
        }
        return genomeAssemblyBuilder.build();
    }

    private GenomeAnnotation getGenomeAnnotation(Row row) {
        GenomeAnnotationBuilder genomeAnnotationBuilder = new GenomeAnnotationBuilder();
        genomeAnnotationBuilder.source(row.getString(row.fieldIndex(GENOME_ANNOTATION_SOURCE)));
        if (hasFieldName(GENOME_ANNOTATION_URL, row)) {
            genomeAnnotationBuilder.url(row.getString(row.fieldIndex(GENOME_ANNOTATION_URL)));
        }
        return genomeAnnotationBuilder.build();
    }

    @Data
    private static class Score {
        private final String name;
        private final List<Property> properties;
    }

    @Data
    private static class Property {
        private final String name;
        private final String value;
    }
}
