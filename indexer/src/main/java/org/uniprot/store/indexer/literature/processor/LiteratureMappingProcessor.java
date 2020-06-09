package org.uniprot.store.indexer.literature.processor;

import java.nio.ByteBuffer;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureStoreEntry;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.core.literature.impl.LiteratureStoreEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStoreEntryImpl;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureMappingProcessor
        implements ItemProcessor<LiteratureStoreEntry, LiteratureDocument> {

    private final UniProtSolrClient uniProtSolrClient;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureMappingProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureStoreEntry mappedEntry) throws Exception {
        Literature literature = (Literature) mappedEntry.getLiteratureEntry().getCitation();
        SolrQuery query = new SolrQuery("id:" + literature.getPubmedId());
        Optional<LiteratureDocument> optionalDocument =
                uniProtSolrClient.queryForObject(
                        SolrCollection.literature, query, LiteratureDocument.class);
        LiteratureStatisticsBuilder statisticsBuilder = new LiteratureStatisticsBuilder();
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            // Get statistics from previous step and copy it to builder
            byte[] literatureObj = document.getLiteratureObj().array();
            LiteratureStoreEntry statisticsEntry =
                    literatureObjectMapper.readValue(literatureObj, LiteratureStoreEntryImpl.class);
            statisticsBuilder =
                    LiteratureStatisticsBuilder.from(
                            statisticsEntry.getLiteratureEntry().getStatistics());
        }
        // update mappedProteinCount in the statistic builder
        statisticsBuilder.mappedProteinCount(mappedEntry.getLiteratureMappedReferences().size());

        // Set updated statistics to the mappedEntry
        LiteratureStoreEntryBuilder mappedEntryStoreBuilder =
                LiteratureStoreEntryBuilder.from(mappedEntry);
        LiteratureEntryBuilder entryBuilder =
                LiteratureEntryBuilder.from(mappedEntry.getLiteratureEntry());
        entryBuilder.statistics(statisticsBuilder.build());
        mappedEntryStoreBuilder.literatureEntry(entryBuilder.build());
        mappedEntry = mappedEntryStoreBuilder.build();

        return createLiteratureDocument(mappedEntry, literature.getPubmedId());
    }

    private LiteratureDocument createLiteratureDocument(
            LiteratureStoreEntry mappedEntry, Long pubmedId) {
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        byte[] literatureByte = getLiteratureObjectBinary(mappedEntry);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));
        builder.id(String.valueOf(pubmedId));

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
