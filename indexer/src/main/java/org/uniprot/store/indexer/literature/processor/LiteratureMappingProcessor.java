package org.uniprot.store.indexer.literature.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.builder.LiteratureEntryBuilder;
import org.uniprot.core.literature.builder.LiteratureStatisticsBuilder;
import org.uniprot.core.literature.impl.LiteratureEntryImpl;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author lgonzales
 */
@Slf4j
public class LiteratureMappingProcessor implements ItemProcessor<LiteratureEntry, LiteratureDocument> {

    private final UniProtSolrOperations solrOperations;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureMappingProcessor(UniProtSolrOperations solrOperations) {
        this.solrOperations = solrOperations;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureEntry mappedEntry) throws Exception {
        Query query = new SimpleQuery().addCriteria(Criteria.where("id").is(mappedEntry.getPubmedId()));
        Optional<LiteratureDocument> optionalDocument = solrOperations.queryForObject(SolrCollection.literature.name(), query, LiteratureDocument.class);
        LiteratureStatisticsBuilder statisticsBuilder = new LiteratureStatisticsBuilder();
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            //Get statistics from previous step and copy it to builder
            byte[] literatureObj = document.getLiteratureObj().array();
            LiteratureEntry statisticsEntry = literatureObjectMapper.readValue(literatureObj, LiteratureEntryImpl.class);
            statisticsBuilder = statisticsBuilder.from(statisticsEntry.getStatistics());

            solrOperations.delete(SolrCollection.literature.name(), query);
            solrOperations.softCommit(SolrCollection.literature.name());
        }
        //update mappedProteinCount in the statistic builder
        statisticsBuilder.mappedProteinCount(mappedEntry.getLiteratureMappedReferences().size());

        //Set updated statistics to the mappedEntry
        LiteratureEntryBuilder mappedEntryBuilder = new LiteratureEntryBuilder().from(mappedEntry);
        mappedEntryBuilder.statistics(statisticsBuilder.build());
        mappedEntry = mappedEntryBuilder.build();

        return createLiteratureDocument(mappedEntry);
    }

    private LiteratureDocument createLiteratureDocument(LiteratureEntry mappedEntry) {
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        byte[] literatureByte = getLiteratureObjectBinary(mappedEntry);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));
        builder.id(String.valueOf(mappedEntry.getPubmedId()));

        log.debug("LiteratureStatisticsProcessor entry: " + mappedEntry);
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
