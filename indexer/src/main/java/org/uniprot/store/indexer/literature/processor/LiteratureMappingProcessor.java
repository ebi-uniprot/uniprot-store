package org.uniprot.store.indexer.literature.processor;

import java.nio.ByteBuffer;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureStoreEntry;
import org.uniprot.core.literature.builder.LiteratureEntryBuilder;
import org.uniprot.core.literature.builder.LiteratureStatisticsBuilder;
import org.uniprot.core.literature.builder.LiteratureStoreEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStoreEntryImpl;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureMappingProcessor
        implements ItemProcessor<LiteratureStoreEntry, LiteratureDocument> {

    private final UniProtSolrOperations solrOperations;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureMappingProcessor(UniProtSolrOperations solrOperations) {
        this.solrOperations = solrOperations;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureStoreEntry mappedEntry) throws Exception {
        Query query =
                new SimpleQuery()
                        .addCriteria(
                                Criteria.where("id")
                                        .is(mappedEntry.getLiteratureEntry().getPubmedId()));
        Optional<LiteratureDocument> optionalDocument =
                solrOperations.queryForObject(
                        SolrCollection.literature.name(), query, LiteratureDocument.class);
        LiteratureStatisticsBuilder statisticsBuilder = new LiteratureStatisticsBuilder();
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            // Get statistics from previous step and copy it to builder
            byte[] literatureObj = document.getLiteratureObj().array();
            LiteratureStoreEntry statisticsEntry =
                    literatureObjectMapper.readValue(literatureObj, LiteratureStoreEntryImpl.class);
            statisticsBuilder =
                    statisticsBuilder.from(statisticsEntry.getLiteratureEntry().getStatistics());
        }
        // update mappedProteinCount in the statistic builder
        statisticsBuilder.mappedProteinCount(mappedEntry.getLiteratureMappedReferences().size());

        // Set updated statistics to the mappedEntry
        LiteratureStoreEntryBuilder mappedEntryStoreBuilder =
                new LiteratureStoreEntryBuilder().from(mappedEntry);
        LiteratureEntryBuilder entryBuilder =
                new LiteratureEntryBuilder().from(mappedEntry.getLiteratureEntry());
        entryBuilder.statistics(statisticsBuilder.build());
        mappedEntryStoreBuilder.literatureEntry(entryBuilder.build());
        mappedEntry = mappedEntryStoreBuilder.build();

        return createLiteratureDocument(mappedEntry);
    }

    private LiteratureDocument createLiteratureDocument(LiteratureStoreEntry mappedEntry) {
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        byte[] literatureByte = getLiteratureObjectBinary(mappedEntry);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));
        builder.id(String.valueOf(mappedEntry.getLiteratureEntry().getPubmedId()));

        log.debug("LiteratureStatisticsProcessor entry: " + mappedEntry);
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
