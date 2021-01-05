package org.uniprot.store.indexer.uniprotkb.converter;

import static org.uniprot.core.publication.MappedReferenceType.UNIPROTKB_REVIEWED;
import static org.uniprot.core.publication.MappedReferenceType.UNIPROTKB_UNREVIEWED;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.JournalArticle;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.publication.UniProtKBMappedReference;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.core.publication.impl.MappedSourceBuilder;
import org.uniprot.core.publication.impl.UniProtKBMappedReferenceBuilder;
import org.uniprot.core.uniprotkb.ReferenceComment;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.core.util.PublicationDateFormatter;
import org.uniprot.store.indexer.publication.common.PublicationUtils;
import org.uniprot.store.search.document.publication.PublicationDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
@Slf4j
public class UniProtEntryReferencesConverter implements Serializable {

    private static final long serialVersionUID = -6569918266610976310L;
    private ObjectMapper objectMapper;

    public UniProtEntryReferencesConverter() {
        this.objectMapper = MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();
    }

    public List<PublicationDocument> convertToPublicationDocuments(UniProtKBEntry uniProtKBEntry) {
        String accession = uniProtKBEntry.getPrimaryAccession().getValue();
        List<UniProtKBReference> references = uniProtKBEntry.getReferences();
        List<PublicationDocument> pubDocs = new ArrayList<>();
        for (int i = 0; i < references.size(); i++) {
            UniProtKBReference reference = references.get(i);
            Citation citation = reference.getCitation();
            Optional<CrossReference<CitationDatabase>> optPubMed =
                    citation.getCitationCrossReferenceByType(CitationDatabase.PUBMED);
            if (isEntryTypeSupported(uniProtKBEntry)) {
                String pubmedId = optPubMed.isPresent() ? optPubMed.get().getId() : null;
                MappedReferenceType type =
                        uniProtKBEntry.getEntryType() == UniProtKBEntryType.SWISSPROT
                                ? UNIPROTKB_REVIEWED
                                : UNIPROTKB_UNREVIEWED;

                int referenceNumber = i + 1; // RN[]

                // create MappedPublications to store binary
                UniProtKBMappedReference mappedReference =
                        createUniProtKBMappedReference(
                                uniProtKBEntry, reference, pubmedId, referenceNumber);
                MappedPublications mappedPubs = createMappedPublications(mappedReference, type);

                String id = PublicationUtils.getDocumentId();
                byte[] mappedReferenceByte = getMappedPublicationsBinary(mappedPubs);
                PublicationDocument.PublicationDocumentBuilder builder =
                        PublicationDocument.builder();
                builder.id(id);
                builder.refNumber(referenceNumber);
                builder.accession(accession);
                builder.type(type.getIntValue());
                builder.mainType(type.getIntValue());
                builder.pubMedId(pubmedId);
                builder.categories(mappedReference.getSourceCategories());
                builder.publicationMappedReferences(mappedReferenceByte);
                pubDocs.add(builder.build());
            }
        }
        return pubDocs;
    }

    public byte[] getMappedPublicationsBinary(MappedPublications mappedPublications) {
        try {
            return objectMapper.writeValueAsBytes(mappedPublications);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse MappedPublications to binary json: ", e);
        }
    }

    private boolean isEntryTypeSupported(UniProtKBEntry uniProtKBEntry) {
        return uniProtKBEntry.getEntryType() == UniProtKBEntryType.SWISSPROT
                || uniProtKBEntry.getEntryType() == UniProtKBEntryType.TREMBL;
    }

    private UniProtKBMappedReference createUniProtKBMappedReference(
            UniProtKBEntry uniProtKBEntry,
            UniProtKBReference reference,
            String pubMedId,
            int referenceNumber) {
        String source = uniProtKBEntry.getEntryType().getDisplayName();
        UniProtKBMappedReferenceBuilder builder = new UniProtKBMappedReferenceBuilder();
        builder.uniProtKBAccession(uniProtKBEntry.getPrimaryAccession());
        builder.source(new MappedSourceBuilder().name(source).build());
        builder.pubMedId(pubMedId);
        builder.referenceNumber(referenceNumber);
        builder.referenceCommentsSet(reference.getReferenceComments());
        builder.referencePositionsSet(reference.getReferencePositions());
        builder.sourceCategoriesSet(getCategoriesFromUniprotReference(reference));
        return builder.build();
    }

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
            }
            if (citation.hasAuthors()) {
                citation.getAuthors()
                        .forEach(
                                author -> {
                                    document.referenceAuthors.add(author.getValue());
                                });
            }
            if (citation.hasAuthoringGroup()) {
                citation.getAuthoringGroups()
                        .forEach(
                                authGroup -> {
                                    document.referenceOrganizations.add(authGroup);
                                });
            }
            if (citation.hasPublicationDate()) {
                convertPublicationDate(citation.getPublicationDate().getValue(), document);
            }
            citation.getCitationCrossReferenceByType(CitationDatabase.PUBMED)
                    .ifPresent(
                            pubmed -> {
                                document.referencePubmeds.add(pubmed.getId());
                            });
            if (citation instanceof JournalArticle) {
                JournalArticle ja = (JournalArticle) citation;
                document.referenceJournals.add(ja.getJournal().getName());
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
                    }
                });
    }

    private void convertReferencePositions(
            UniProtKBReference uniProtkbReference, UniProtDocument document) {
        if (uniProtkbReference.hasReferencePositions()) {
            List<String> positions = uniProtkbReference.getReferencePositions();
            document.scopes.addAll(positions);
        }
    }

    private Set<String> getCategoriesFromUniprotReference(UniProtKBReference uniProtkbReference) {
        Set<String> result = new HashSet<>();
        if (uniProtkbReference.hasReferencePositions()) {
            for (String position : uniProtkbReference.getReferencePositions()) {
                for (PublicationCategory category : PublicationCategory.values()) {
                    for (String categoryText : category.getFunctionTexts()) {
                        if (position.toUpperCase().contains(categoryText)) {
                            result.add(category.getLabel());
                        }
                    }
                }
            }
        }
        return result;
    }

    private MappedPublications createMappedPublications(
            UniProtKBMappedReference mappedReference, MappedReferenceType type) {
        MappedPublicationsBuilder mappedPubsBuilder = new MappedPublicationsBuilder();
        if (type == UNIPROTKB_UNREVIEWED) {
            mappedPubsBuilder.unreviewedMappedReference(mappedReference);
        } else {
            mappedPubsBuilder.reviewedMappedReference(mappedReference);
        }
        return mappedPubsBuilder.build();
    }
}
