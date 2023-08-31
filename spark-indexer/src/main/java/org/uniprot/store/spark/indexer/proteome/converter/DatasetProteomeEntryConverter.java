package org.uniprot.store.spark.indexer.proteome.converter;

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
import org.uniprot.core.proteome.impl.ComponentBuilder;
import org.uniprot.core.proteome.impl.GenomeAnnotationBuilder;
import org.uniprot.core.proteome.impl.GenomeAssemblyBuilder;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.uniprot.store.spark.indexer.common.util.RowUtils.hasFieldName;
import static org.uniprot.store.spark.indexer.proteome.ProteomeRDDReader.*;

/**
 * Converts XML {@link Row} instances to {@link ProteomeEntry} instances.
 *
 * @author sahmad
 * @created 21/08/2020
 */
public class DatasetProteomeEntryConverter implements Function<Row, ProteomeEntry>, Serializable {

    private static final long serialVersionUID = -6073762696467389831L;

    @Override
    public ProteomeEntry call(Row row) throws Exception {
        ProteomeEntryBuilder builder = new ProteomeEntryBuilder();
        // upid
        if (hasFieldName(UPID, row)) {
            builder.proteomeId(row.getString(row.fieldIndex(UPID)));
        }
        //taxonomy
        if (hasFieldName(TAXONOMY, row)) {
            Taxonomy taxonomy = new TaxonomyBuilder().taxonId(row.getInt(row.fieldIndex(TAXONOMY))).build();
            builder.taxonomy(taxonomy);
        }
        //strain
        if (hasFieldName(STRAIN, row)) {
            builder.strain(row.getString(row.fieldIndex(STRAIN)));
        }
        //strain
        if (hasFieldName(DESCRIPTION, row)) {
            builder.description(row.getString(row.fieldIndex(DESCRIPTION)));
        }
        //modified
        if (hasFieldName(MODIFIED, row)) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            builder.modified(LocalDate.parse(row.getString(row.fieldIndex(MODIFIED)), formatter));
        }
        // proteome type
        ProteomeType proteomeType = ProteomeType.NORMAL;
        boolean isReference = false;
        boolean isRepresentative = false;
        if (hasFieldName(IS_REFERENCE_PROTEOME, row)) {
            isReference = row.getBoolean(row.fieldIndex(IS_REFERENCE_PROTEOME));
        }
        if (hasFieldName(IS_REPRESENTATIVE_PROTEOME, row)) {
            isRepresentative = row.getBoolean(row.fieldIndex(IS_REPRESENTATIVE_PROTEOME));
        }
        if (isReference && isRepresentative) {
            proteomeType = ProteomeType.REFERENCE_AND_REPRESENTATIVE;
        } else if (isReference) {
            proteomeType = ProteomeType.REFERENCE;
        } else if (isRepresentative) {
            proteomeType = ProteomeType.REPRESENTATIVE;
        }
        builder.proteomeType(proteomeType);
        //genome annotation
        if (hasFieldName(GENOME_ANNOTATION, row)) {
            GenomeAnnotation genomeAnnotation = getGenomeAnnotation((Row) row.get(row.fieldIndex(GENOME_ANNOTATION)));
            builder.genomeAnnotation(genomeAnnotation);
        }
        //genome assembly
        if (hasFieldName(GENOME_ASSEMBLY, row)) {
            GenomeAssembly genomeAssembly = getGenomeAssembly((Row) row.get(row.fieldIndex(GENOME_ASSEMBLY)));
            builder.genomeAssembly(genomeAssembly);
        }
        //annotation score
        if (hasFieldName(ANNOTATION_SCORE, row)) {
            Integer annotationScore = getAnnotationScore((Row) row.get(row.fieldIndex(ANNOTATION_SCORE)));
            builder.annotationScore(annotationScore);
        }
        //component set
        if (hasFieldName(COMPONENT, row)) {
            List<Row> componentRows = row.getList(row.fieldIndex(COMPONENT));
            builder.componentsSet(componentRows.stream().map(this::getComponent).collect(Collectors.toList()));
        }
        //citation set
        if (hasFieldName(REFERENCE, row)) {
            List<Row> referenceRows = row.getList(row.fieldIndex(REFERENCE));
            builder.citationsSet(referenceRows.stream().map(this::getCitation).filter(Objects::nonNull).collect(Collectors.toList()));
        }

        return builder.build();
    }

    private Citation getCitation(Row row) {
        if (hasFieldName(CITATION, row)) {
            Row citationRow = (Row) row.get(row.fieldIndex(CITATION));
            return getCitationItem(citationRow);
        }
        return null;
    }

    private Citation getCitationItem(Row row) {
        if (hasFieldName(TYPE, row)) {
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
        }
        return null;
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
            submissionBuilder.submittedToDatabase(SubmissionDatabase.typeOf(row.getString(row.fieldIndex(DB))));
        }
        return submissionBuilder.build();
    }

    private Citation getJournalArticle(Row row) {
        JournalArticleBuilder journalArticleBuilder = new JournalArticleBuilder();
        populateCommon(row, journalArticleBuilder);
        if (hasFieldName(FIRST, row)) {
            journalArticleBuilder.firstPage(String.valueOf(row.getLong(row.fieldIndex(FIRST))));
        }
        if (hasFieldName(LAST, row)) {
            journalArticleBuilder.lastPage(String.valueOf(row.getLong(row.fieldIndex(LAST))));
        }
        if (hasFieldName(VOLUME, row)) {
            journalArticleBuilder.volume(String.valueOf(row.getLong(row.fieldIndex(VOLUME))));
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

    private void populateCommon(Row row, AbstractCitationBuilder citationBuilder) {
        if (hasFieldName(AUTHOR_LIST, row)) {
            Row authorsRow = (Row) row.get(row.fieldIndex(AUTHOR_LIST));
            citationBuilder.authorsSet(getAuthors(authorsRow));
        }
        if (hasFieldName(DB_REFERENCE, row)) {
            List<Row> dbReferences = row.getList(row.fieldIndex(DB_REFERENCE));
            citationBuilder.citationCrossReferencesSet(dbReferences.stream().map(this::getCrossRef).filter(Objects::nonNull).collect(Collectors.toList()));
        }
        if (hasFieldName(TITLE, row)) {
            citationBuilder.title((row.getString(row.fieldIndex(TITLE))));
        }
        if (hasFieldName(DATE, row)) {
            citationBuilder.publicationDate((row.getString(row.fieldIndex(DATE))));
        }
    }

    private CrossReference<CitationDatabase> getCrossRef(Row row) {
        if (hasFieldName(TYPE, row)) {
            return new CrossReferenceBuilder<CitationDatabase>()
                    .database(CitationDatabase.typeOf(row.getString(row.fieldIndex(TYPE))))
                    .id(row.getString(row.fieldIndex(ID)))
                    .build();
        }
        return null;
    }

    private Collection<String> getAuthors(Row row) {
        List<String> authorList = new LinkedList<>();
        if (hasFieldName(PERSON, row)) {
            List<Row> personList = row.getList(row.fieldIndex(PERSON));
            authorList.addAll(personList.stream().map(this::getName).filter(Objects::nonNull).collect(Collectors.toList()));
        }
        return authorList;
    }

    private String getName(Row row) {
        if (hasFieldName(NAME, row)) {
            return row.getString(row.fieldIndex(NAME));
        }
        return null;
    }

    private Integer getAnnotationScore(Row row) {
        Integer annotationScore = null;
        if (hasFieldName(NORMALIZED_ANNOTATION_SCORE, row)) {
            annotationScore = (int) row.getLong(row.fieldIndex(NORMALIZED_ANNOTATION_SCORE));
        }
        return annotationScore;
    }

    private Component getComponent(Row row) {
        ComponentBuilder componentBuilder = new ComponentBuilder();
        if (hasFieldName(NAME, row)) {
            componentBuilder.name(row.getString(row.fieldIndex(NAME)));
        }
        if (hasFieldName(PROTEIN_COUNT, row)) {
            componentBuilder.proteinCount((int) row.getLong(row.fieldIndex(PROTEIN_COUNT)));
        }
        if (hasFieldName(DESCRIPTION, row)) {
            componentBuilder.description(row.getString(row.fieldIndex(DESCRIPTION)));
        }
        if (hasFieldName(GENOME_ANNOTATION, row)) {
            GenomeAnnotation genomeAnnotation = getGenomeAnnotation((Row) row.get(row.fieldIndex(GENOME_ANNOTATION)));
            componentBuilder.genomeAnnotation(genomeAnnotation);
        }
        return componentBuilder.build();
    }

    private GenomeAssembly getGenomeAssembly(Row row) {
        GenomeAssemblyBuilder genomeAssemblyBuilder = new GenomeAssemblyBuilder();
        if (hasFieldName(GENOME_ASSEMBLY, row)) {
            genomeAssemblyBuilder.assemblyId(row.getString(row.fieldIndex(GENOME_ASSEMBLY)));
        }
        if (hasFieldName(GENOME_ASSEMBLY_SOURCE, row)) {
            genomeAssemblyBuilder.source(GenomeAssemblySource.fromValue((row.getString(row.fieldIndex(GENOME_ASSEMBLY_SOURCE)))));
        }
        if (hasFieldName(GENOME_ASSEMBLY_URL, row)) {
            genomeAssemblyBuilder.genomeAssemblyUrl(row.getString(row.fieldIndex(GENOME_ASSEMBLY_URL)));
        }
        if (hasFieldName(GENOME_REPRESENTATION, row)) {
            genomeAssemblyBuilder.level(GenomeAssemblyLevel.fromValue(row.getString(row.fieldIndex(GENOME_REPRESENTATION))));
        }
        return genomeAssemblyBuilder.build();
    }

    private GenomeAnnotation getGenomeAnnotation(Row row) {
        GenomeAnnotationBuilder genomeAnnotationBuilder = new GenomeAnnotationBuilder();
        if (hasFieldName(GENOME_ANNOTATION_SOURCE, row)) {
            genomeAnnotationBuilder.source(row.getString(row.fieldIndex(GENOME_ANNOTATION_SOURCE)));
        }
        if (hasFieldName(GENOME_ANNOTATION_URL, row)) {
            genomeAnnotationBuilder.url(row.getString(row.fieldIndex(GENOME_ANNOTATION_URL)));
        }
        return genomeAnnotationBuilder.build();
    }
}
