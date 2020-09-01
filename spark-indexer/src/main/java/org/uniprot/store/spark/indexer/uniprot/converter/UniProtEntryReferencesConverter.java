package org.uniprot.store.spark.indexer.uniprot.converter;

import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.JournalArticle;
import org.uniprot.core.uniprotkb.ReferenceComment;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.core.util.PublicationDateFormatter;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
@Slf4j
class UniProtEntryReferencesConverter {

    UniProtEntryReferencesConverter() {}

    void convertReferences(List<UniProtKBReference> references, UniProtDocument document) {
        for (UniProtKBReference reference : references) {
            Citation citation = reference.getCitation();
            if (reference.hasReferenceComments()) {
                convertReferenceComments(reference.getReferenceComments(), document);
            }
            if (reference.hasReferencePositions()) {
                convertReferencePositions(reference, document);
            }
            if (citation.hasTitle()) {
                document.referenceTitles.add(citation.getTitle());
//                document.content.add(citation.getTitle());
            }
            if (citation.hasAuthors()) {
                citation.getAuthors()
                        .forEach(
                                author -> {
                                    document.referenceAuthors.add(author.getValue());
//                                    document.content.add(author.getValue());
                                });
            }
            if (citation.hasAuthoringGroup()) {
                citation.getAuthoringGroups()
                        .forEach(
                                authGroup -> {
                                    document.referenceOrganizations.add(authGroup);
//                                    document.content.add(authGroup);
                                });
            }
            if (citation.hasPublicationDate()) {
                convertPublicationDate(citation.getPublicationDate().getValue(), document);
            }
            Optional<CrossReference<CitationDatabase>> pubmedCitation =
                    citation.getCitationCrossReferenceByType(CitationDatabase.PUBMED);
            if (pubmedCitation.isPresent()) {
                CrossReference<CitationDatabase> pubmed = pubmedCitation.get();
                document.referencePubmeds.add(pubmed.getId());
//                document.content.add(pubmed.getId());
            }
            if (citation instanceof JournalArticle) {
                JournalArticle ja = (JournalArticle) citation;
                document.referenceJournals.add(ja.getJournal().getName());
//                document.content.add(ja.getJournal().getName());
            }
        }
    }

    @SuppressWarnings("squid:S2259")
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
            if (Utils.notNull(dateFormatter)) {
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
//                        document.content.add(commentValue);
                    }
                });
    }

    private void convertReferencePositions(
            UniProtKBReference uniProtkbReference, UniProtDocument document) {
        if (uniProtkbReference.hasReferencePositions()) {
            List<String> positions = uniProtkbReference.getReferencePositions();
            document.scopes.addAll(positions);
//            document.content.addAll(positions);
        }
    }
}
