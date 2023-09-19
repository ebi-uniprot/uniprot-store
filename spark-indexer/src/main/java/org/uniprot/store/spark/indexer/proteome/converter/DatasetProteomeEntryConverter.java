package org.uniprot.store.spark.indexer.proteome.converter;

import org.apache.commons.collections.CollectionUtils;
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

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
    @Override
    public ProteomeEntry call(Row row) throws Exception {
        ProteomeEntryBuilder builder = new ProteomeEntryBuilder();
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
        if (hasFieldName(REDUNDANT_TO, row)) {
            builder.redundantTo(
                    new ProteomeIdBuilder((row.getString(row.fieldIndex(REDUNDANT_TO)))).build());
        }
        if (hasFieldName(PANPROTEOME, row)) {
            builder.panproteome(
                    new ProteomeIdBuilder((row.getString(row.fieldIndex(PANPROTEOME)))).build());
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        builder.modified(
                LocalDate.parse(row.getDate(row.fieldIndex(MODIFIED)).toString(), formatter));
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
        List<Row> redundantProteomeRows = null;
        if (hasFieldName(REDUNDANT_PROTEOME, row)) {
            redundantProteomeRows = row.getList(row.fieldIndex(REDUNDANT_PROTEOME));
            builder.redundantProteomesSet(
                    redundantProteomeRows.stream()
                            .map(this::getRedundantProteome)
                            .collect(Collectors.toList()));
        }
        ExclusionReason exclusionReason = null;
        if (hasFieldName(EXCLUDED, row)) {
            exclusionReason = getExclusionReason((Row) row.get(row.fieldIndex(EXCLUDED)));
            builder.exclusionReasonsAdd(exclusionReason);
        }
        boolean isReference = row.getBoolean(row.fieldIndex(IS_REFERENCE_PROTEOME));
        boolean isRepresentative = row.getBoolean(row.fieldIndex(IS_REPRESENTATIVE_PROTEOME));
        builder.proteomeType(
                getProteomeType(
                        isReference,
                        isRepresentative,
                        Objects.nonNull(exclusionReason),
                        CollectionUtils.isEmpty((redundantProteomeRows))));

        return builder.build();
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
        redundantProteomeBuilder.proteomeId(row.getString(row.fieldIndex(UPID)));
        redundantProteomeBuilder.similarity(
                Float.parseFloat(row.getString(row.fieldIndex(SIMILARITY))));
        return redundantProteomeBuilder.build();
    }

    private ExclusionReason getExclusionReason(Row row) {
        return ExclusionReason.typeOf(row.getString(row.fieldIndex(EXCLUSION_REASON)));
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
            bookBuilder.firstPage(String.valueOf(row.getLong(row.fieldIndex(FIRST))));
        }
        if (hasFieldName(LAST, row)) {
            bookBuilder.lastPage(String.valueOf(row.getLong(row.fieldIndex(LAST))));
        }
        if (hasFieldName(VOLUME, row)) {
            bookBuilder.volume(String.valueOf(row.getLong(row.fieldIndex(VOLUME))));
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
            if(hasFieldName(PERSON,authorsRow)) {
                citationBuilder.authorsSet(getAuthors(authorsRow));
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
        return componentBuilder.build();
    }

    private GenomeAssembly getGenomeAssembly(Row row) {
        GenomeAssemblyBuilder genomeAssemblyBuilder = new GenomeAssemblyBuilder();
        genomeAssemblyBuilder.assemblyId(row.getString(row.fieldIndex(GENOME_ASSEMBLY)));
        genomeAssemblyBuilder.source(
                GenomeAssemblySource.fromValue(
                        (row.getString(row.fieldIndex(GENOME_ASSEMBLY_SOURCE)))));
        if (hasFieldName(GENOME_ASSEMBLY_URL, row)) {
            genomeAssemblyBuilder.genomeAssemblyUrl(
                    row.getString(row.fieldIndex(GENOME_ASSEMBLY_URL)));
        }
        genomeAssemblyBuilder.level(
                GenomeAssemblyLevel.fromValue(
                        row.getString(row.fieldIndex(GENOME_REPRESENTATION))));
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
}
