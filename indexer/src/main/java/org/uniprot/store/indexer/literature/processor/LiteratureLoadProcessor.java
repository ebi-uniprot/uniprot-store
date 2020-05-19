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
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.LiteratureStoreEntry;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStoreEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStoreEntryImpl;
import org.uniprot.core.uniprotkb.UniProtKBAccession;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureLoadProcessor implements ItemProcessor<LiteratureEntry, LiteratureDocument> {

    private final UniProtSolrClient solrClient;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureLoadProcessor(UniProtSolrClient solrClient) {
        this.solrClient = solrClient;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureEntry entry) throws Exception {
        LiteratureStoreEntryBuilder entryStoreBuilder = new LiteratureStoreEntryBuilder();
        LiteratureEntryBuilder entryBuilder = LiteratureEntryBuilder.from(entry);
        Literature literature = (Literature) entry.getCitation();
        SolrQuery query = new SolrQuery("id:" + literature.getPubmedId());
        Optional<LiteratureDocument> optionalDocument =
                solrClient.queryForObject(
                        SolrCollection.literature, query, LiteratureDocument.class);
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
        Literature literature = (Literature) entry.getCitation();
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        Set<String> content = new HashSet<>();
        builder.id(String.valueOf(literature.getPubmedId()));
        content.add(String.valueOf(literature.getPubmedId()));

        builder.doi(literature.getDoiId());
        content.add(literature.getDoiId());

        builder.title(literature.getTitle());
        content.add(literature.getTitle());

        if (literature.hasAuthors()) {
            Set<String> authors =
                    literature.getAuthors().stream()
                            .map(Author::getValue)
                            .collect(Collectors.toSet());
            builder.author(authors);
            content.addAll(authors);
        }
        if (literature.hasJournal()) {
            builder.journal(literature.getJournal().getName());
            content.add(literature.getJournal().getName());
        }
        if (literature.hasPublicationDate()) {
            builder.published(literature.getPublicationDate().getValue());
        }
        if (entry.hasStatistics()) {
            LiteratureStatistics statistics = entry.getStatistics();
            builder.mappedin(statistics.hasMappedProteinCount());
            builder.citedin(
                    statistics.hasReviewedProteinCount() || statistics.hasUnreviewedProteinCount());
        }

        if (literature.hasLiteratureAbstract()) {
            content.add(literature.getLiteratureAbstract());
        }
        if (literature.hasAuthoringGroup()) {
            content.addAll(literature.getAuthoringGroups());
        }
        builder.content(content);

        if (entryStore.hasLiteratureMappedReferences()) {
            Set<String> uniprotAccessions =
                    entryStore.getLiteratureMappedReferences().stream()
                            .filter(LiteratureMappedReference::hasUniprotAccession)
                            .map(LiteratureMappedReference::getUniprotAccession)
                            .map(UniProtKBAccession::getValue)
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
