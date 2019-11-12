package org.uniprot.store.indexer.uniprotkb.converter;

import lombok.extern.slf4j.Slf4j;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationXrefType;
import org.uniprot.core.citation.JournalArticle;
import org.uniprot.core.uniprot.ReferenceComment;
import org.uniprot.core.uniprot.UniProtReference;
import org.uniprot.core.util.PublicationDateFormatter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.List;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
@Slf4j
class UniProtEntryReferencesConverter {

    UniProtEntryReferencesConverter() {}

    void convertReferences(List<UniProtReference> references, UniProtDocument document) {
        for (UniProtReference reference : references) {
            Citation citation = reference.getCitation();
            if (reference.hasReferenceComments()) {
                convertReferenceComments(reference.getReferenceComments(), document);
            }
            if (reference.hasReferencePositions()) {
                convertReferencePositions(reference, document);
            }
            if (citation.hasTitle()) {
                document.referenceTitles.add(citation.getTitle());
                document.content.add(citation.getTitle());
            }
            if (citation.hasAuthors()) {
                citation.getAuthors()
                        .forEach(
                                author -> {
                                    document.referenceAuthors.add(author.getValue());
                                    document.content.add(author.getValue());
                                });
            }
            if (citation.hasAuthoringGroup()) {
                citation.getAuthoringGroup()
                        .forEach(
                                authGroup -> {
                                    document.referenceOrganizations.add(authGroup);
                                    document.content.add(authGroup);
                                });
            }
            if (citation.hasPublicationDate()) {
                convertPublicationDate(citation.getPublicationDate().getValue(), document);
            }
            citation.getCitationXrefsByType(CitationXrefType.PUBMED)
                    .ifPresent(
                            pubmed -> {
                                document.referencePubmeds.add(pubmed.getId());
                                document.content.add(pubmed.getId());
                            });
            if (citation instanceof JournalArticle) {
                JournalArticle ja = (JournalArticle) citation;
                document.referenceJournals.add(ja.getJournal().getName());
                document.content.add(ja.getJournal().getName());
            }
        }
    }

    private void convertPublicationDate(String publicationDate, UniProtDocument document) {
        try {
            PublicationDateFormatter dateFormatter = null;
            if (PublicationDateFormatter.DAY_DIGITMONTH_YEAR.isValidDate(publicationDate)) {
                dateFormatter = PublicationDateFormatter.DAY_DIGITMONTH_YEAR;
            } else if (PublicationDateFormatter.DAY_THREE_LETTER_MONTH_YEAR.isValidDate(
                    publicationDate)) {
                dateFormatter = PublicationDateFormatter.DAY_THREE_LETTER_MONTH_YEAR;
            } else if (PublicationDateFormatter.YEAR_DIGIT_MONTH.isValidDate(publicationDate)) {
                dateFormatter = PublicationDateFormatter.YEAR_DIGIT_MONTH;
            } else if (PublicationDateFormatter.THREE_LETTER_MONTH_YEAR.isValidDate(
                    publicationDate)) {
                dateFormatter = PublicationDateFormatter.THREE_LETTER_MONTH_YEAR;
            } else if (PublicationDateFormatter.YEAR.isValidDate(publicationDate)) {
                dateFormatter = PublicationDateFormatter.YEAR;
            }
            if (dateFormatter != null) {
                document.referenceDates.add(dateFormatter.convertStringToDate(publicationDate));
            }
        } catch (Exception e) {
            log.warn("There was a problem converting entry dates during indexing:", e);
        }
    }

    private void convertReferenceComments(
            List<ReferenceComment> referenceComments, UniProtDocument document) {
        referenceComments.forEach(
                referenceComment -> {
                    if (referenceComment.hasValue()) {
                        String commentValue = referenceComment.getValue();
                        switch (referenceComment.getType()) {
                            case STRAIN:
                                document.rcStrain.add(commentValue);
                                break;
                            case TISSUE:
                                document.rcTissue.add(commentValue);
                                break;
                            case PLASMID:
                                document.rcPlasmid.add(commentValue);
                                break;
                            case TRANSPOSON:
                                document.rcTransposon.add(commentValue);
                                break;
                        }
                        document.content.add(commentValue);
                    }
                });
    }

    private void convertReferencePositions(
            UniProtReference uniProtReference, UniProtDocument document) {
        if (uniProtReference.hasReferencePositions()) {
            List<String> positions = uniProtReference.getReferencePositions();
            document.scopes.addAll(positions);
            document.content.addAll(positions);
        }
    }
}
