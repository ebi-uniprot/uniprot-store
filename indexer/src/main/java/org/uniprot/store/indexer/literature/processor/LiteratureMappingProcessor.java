package org.uniprot.store.indexer.literature.processor;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureEntryImpl;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureMappingProcessor
        implements ItemProcessor<LiteratureEntry, LiteratureDocument> {

    private final UniProtSolrClient uniProtSolrClient;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureMappingProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureEntry mappedEntry) throws Exception {
        Citation literature = mappedEntry.getCitation();
        SolrQuery query = new SolrQuery("id:" + literature.getId());
        Optional<LiteratureDocument> optionalDocument =
                uniProtSolrClient.queryForObject(
                        SolrCollection.literature, query, LiteratureDocument.class);
        LiteratureStatisticsBuilder statisticsBuilder = new LiteratureStatisticsBuilder();
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            // Get statistics from previous step and copy it to builder
            byte[] literatureObj = document.getLiteratureObj();
            LiteratureEntry statisticsEntry =
                    literatureObjectMapper.readValue(literatureObj, LiteratureEntryImpl.class);
            statisticsBuilder = LiteratureStatisticsBuilder.from(statisticsEntry.getStatistics());
        }

        // update computational mappedProteinCount in the statistic builder
        statisticsBuilder.computationallyMappedProteinCount(
                mappedEntry.getStatistics().getComputationallyMappedProteinCount());

        // update community mappedProteinCount in the statistic builder
        statisticsBuilder.communityMappedProteinCount(
                mappedEntry.getStatistics().getCommunityMappedProteinCount());

        // Set updated statistics to the mappedEntry
        LiteratureEntryBuilder mappedEntryBuilder = LiteratureEntryBuilder.from(mappedEntry);
        mappedEntryBuilder.statistics(statisticsBuilder.build());
        mappedEntry = mappedEntryBuilder.build();

        return createLiteratureDocument(mappedEntry, literature.getId());
    }

    private LiteratureDocument createLiteratureDocument(
            LiteratureEntry mappedEntry, String citationId) {
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        byte[] literatureByte = getLiteratureObjectBinary(mappedEntry);
        builder.literatureObj(literatureByte);
        builder.id(citationId);

        log.debug("LiteratureStatisticsProcessor entry: " + mappedEntry);
        return builder.build();
    }

    private byte[] getLiteratureObjectBinary(LiteratureEntry literature) {
        try {
            return this.literatureObjectMapper.writeValueAsBytes(literature);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException("Unable to parse Literature to binary json: ", e);
        }
    }
}
