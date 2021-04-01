package org.uniprot.store.converter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.*;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 25/03/2021
 */
@Slf4j
public class LiteratureDocumentConverter
        implements DocumentConverter<LiteratureEntry, LiteratureDocument> {

    @Override
    public LiteratureDocument convert(LiteratureEntry entry) {
        Citation literature = entry.getCitation();
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        builder.id(String.valueOf(literature.getId()));

        literature
                .getCitationCrossReferenceByType(CitationDatabase.DOI)
                .map(CrossReference::getId)
                .ifPresent(builder::doi);

        builder.title(literature.getTitle());

        if (literature.hasAuthors()) {
            Set<String> authors =
                    literature.getAuthors().stream()
                            .map(Author::getValue)
                            .collect(Collectors.toSet());
            builder.author(authors);
        }

        if (literature.hasPublicationDate()) {
            builder.published(literature.getPublicationDate().getValue());
        }

        if (entry.hasStatistics()) {
            builder.citationsWith(getCitationsWith(entry.getStatistics()));
        }

        if (literature.hasAuthoringGroup()) {
            builder.authorGroups(new HashSet<>(literature.getAuthoringGroups()));
        }

        if (literature instanceof JournalArticle && (((JournalArticle) literature).hasJournal())) {
            builder.journal(((JournalArticle) literature).getJournal().getName());
        }

        if (literature instanceof Literature
                && (((Literature) literature).hasLiteratureAbstract())) {
            builder.litAbstract(((Literature) literature).getLiteratureAbstract());
        }

        byte[] literatureByte = getLiteratureObjectBinary(entry);
        builder.literatureObj(literatureByte);

        log.debug("LiteratureLoadProcessor entry: " + entry);
        return builder.build();
    }

    private List<String> getCitationsWith(LiteratureStatistics statistics) {
        List<String> citationsWith = new ArrayList<>();
        if (statistics.hasReviewedProteinCount()) {
            citationsWith.add("1_uniprotkb"); // UniProtKB entries
            citationsWith.add("2_reviewed"); // UniProtKB reviewed entries
        }
        if (statistics.hasUnreviewedProteinCount()) {
            citationsWith.add("1_uniprotkb"); // UniProtKB entries
            citationsWith.add("3_unreviewed"); // UniProtKB unreviewed entries
        }
        if (statistics.hasComputationallyMappedProteinCount()) {
            citationsWith.add("4_computationally"); // Computationally mapped entries
        }
        if (statistics.hasCommunityMappedProteinCount()) {
            citationsWith.add("5_community"); // Community mapped entries
        }
        return citationsWith;
    }

    private byte[] getLiteratureObjectBinary(LiteratureEntry literature) {
        ObjectMapper literatureObjectMapper =
                LiteratureJsonConfig.getInstance().getFullObjectMapper();
        try {
            return literatureObjectMapper.writeValueAsBytes(literature);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException("Unable to parse Literature to binary json: ", e);
        }
    }
}
