package org.uniprot.store.indexer.literature.processor;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.citation.Author;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureEntryImpl;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureLoadProcessor implements ItemProcessor<LiteratureEntry, LiteratureDocument> {

    private final UniProtSolrClient uniProtSolrClient;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureLoadProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureEntry entry) throws Exception {
        LiteratureEntryBuilder entryBuilder = LiteratureEntryBuilder.from(entry);
        Literature literature = (Literature) entry.getCitation();
        SolrQuery query = new SolrQuery("id:" + literature.getPubmedId());
        Optional<LiteratureDocument> optionalDocument =
                uniProtSolrClient.queryForObject(
                        SolrCollection.literature, query, LiteratureDocument.class);
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            // Get statistics and mapped references from previous steps and copy it to entry builder
            byte[] literatureObj = document.getLiteratureObj().array();
            LiteratureEntry existingEntry =
                    literatureObjectMapper.readValue(literatureObj, LiteratureEntryImpl.class);
            entryBuilder.statistics(existingEntry.getStatistics());
        }
        return createLiteratureDocument(entryBuilder.build());
    }

    private LiteratureDocument createLiteratureDocument(LiteratureEntry entry) {
        Literature literature = (Literature) entry.getCitation();
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        builder.id(String.valueOf(literature.getPubmedId()));

        builder.doi(literature.getDoiId());

        builder.title(literature.getTitle());

        if (literature.hasAuthors()) {
            Set<String> authors =
                    literature.getAuthors().stream()
                            .map(Author::getValue)
                            .collect(Collectors.toSet());
            builder.author(authors);
        }
        if (literature.hasJournal()) {
            builder.journal(literature.getJournal().getName());
        }
        if (literature.hasPublicationDate()) {
            builder.published(literature.getPublicationDate().getValue());
        }

        if (entry.hasStatistics()) {
            LiteratureStatistics statistics = entry.getStatistics();
            builder.isComputationallyMapped(statistics.hasComputationallyMappedProteinCount());
            builder.isCommunityMapped(statistics.hasCommunityMappedProteinCount());
            builder.isUniprotkbMapped(
                    statistics.hasReviewedProteinCount() || statistics.hasUnreviewedProteinCount());
        }

        if (literature.hasLiteratureAbstract()) {
            builder.litAbstract(literature.getLiteratureAbstract());
        }

        if (literature.hasAuthoringGroup()) {
            builder.authorGroups(new HashSet<>(literature.getAuthoringGroups()));
        }

        byte[] literatureByte = getLiteratureObjectBinary(entry);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));

        log.debug("LiteratureLoadProcessor entry: " + entry);
        return builder.build();
    }

    private byte[] getLiteratureObjectBinary(LiteratureEntry literature) {
        try {
            return this.literatureObjectMapper.writeValueAsBytes(literature);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse Literature to binary json: ", e);
        }
    }
}
