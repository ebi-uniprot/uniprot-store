package org.uniprot.store.indexer.literature.processor;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.uniprot.core.citation.Author;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.LiteratureStoreEntry;
import org.uniprot.core.literature.builder.LiteratureEntryBuilder;
import org.uniprot.core.literature.builder.LiteratureStoreEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStoreEntryImpl;
import org.uniprot.core.uniprot.UniProtAccession;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureLoadProcessor implements ItemProcessor<LiteratureEntry, LiteratureDocument> {

    private final UniProtSolrOperations solrOperations;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureLoadProcessor(UniProtSolrOperations solrOperations) {
        this.solrOperations = solrOperations;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureEntry entry) throws Exception {
        LiteratureStoreEntryBuilder entryStoreBuilder = new LiteratureStoreEntryBuilder();
        LiteratureEntryBuilder entryBuilder = LiteratureEntryBuilder.from(entry);
        Query query = new SimpleQuery().addCriteria(Criteria.where("id").is(entry.getPubmedId()));
        Optional<LiteratureDocument> optionalDocument =
                solrOperations.queryForObject(
                        SolrCollection.literature.name(), query, LiteratureDocument.class);
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            // Get statistics and mapped references from previous steps and copy it to entry builder
            byte[] literatureObj = document.getLiteratureObj().array();
            LiteratureStoreEntry statisticsEntry =
                    literatureObjectMapper.readValue(literatureObj, LiteratureStoreEntryImpl.class);
            entryBuilder.statistics(statisticsEntry.getLiteratureEntry().getStatistics());
            entryStoreBuilder.literatureMappedReferencesSet(
                    statisticsEntry.getLiteratureMappedReferences());
        }
        entryStoreBuilder.literatureEntry(entryBuilder.build());
        return createLiteratureDocument(entryStoreBuilder.build());
    }

    private LiteratureDocument createLiteratureDocument(LiteratureStoreEntry entryStore) {
        LiteratureEntry entry = entryStore.getLiteratureEntry();
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        Set<String> content = new HashSet<>();
        builder.id(String.valueOf(entry.getPubmedId()));
        content.add(String.valueOf(entry.getPubmedId()));

        builder.doi(entry.getDoiId());
        content.add(entry.getDoiId());

        builder.title(entry.getTitle());
        content.add(entry.getTitle());

        if (entry.hasAuthors()) {
            Set<String> authors =
                    entry.getAuthors().stream().map(Author::getValue).collect(Collectors.toSet());
            builder.author(authors);
            content.addAll(authors);
        }
        if (entry.hasJournal()) {
            builder.journal(entry.getJournal().getName());
            content.add(entry.getJournal().getName());
        }
        if (entry.hasPublicationDate()) {
            builder.published(entry.getPublicationDate().getValue());
        }
        if (entry.hasStatistics()) {
            LiteratureStatistics statistics = entry.getStatistics();
            builder.mappedin(statistics.hasMappedProteinCount());
            builder.citedin(
                    statistics.hasReviewedProteinCount() || statistics.hasUnreviewedProteinCount());
        }

        if (entry.hasLiteratureAbstract()) {
            content.add(entry.getLiteratureAbstract());
        }
        if (entry.hasAuthoringGroup()) {
            content.addAll(entry.getAuthoringGroups());
        }
        builder.content(content);

        if (entryStore.hasLiteratureMappedReferences()) {
            Set<String> uniprotAccessions =
                    entryStore.getLiteratureMappedReferences().stream()
                            .filter(LiteratureMappedReference::hasUniprotAccession)
                            .map(LiteratureMappedReference::getUniprotAccession)
                            .map(UniProtAccession::getValue)
                            .collect(Collectors.toSet());
            builder.mappedProteins(uniprotAccessions);
        }

        byte[] literatureByte = getLiteratureObjectBinary(entryStore);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));

        log.debug("LiteratureLoadProcessor entry: " + entry);
        return builder.build();
    }

    private byte[] getLiteratureObjectBinary(LiteratureStoreEntry literature) {
        try {
            return this.literatureObjectMapper.writeValueAsBytes(literature);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse Literature to binary json: ", e);
        }
    }
}
