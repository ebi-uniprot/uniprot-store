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
import org.uniprot.store.converter.LiteratureDocumentConverter;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureLoadProcessor implements ItemProcessor<LiteratureEntry, LiteratureDocument> {

    private final UniProtSolrClient uniProtSolrClient;
    private final ObjectMapper literatureObjectMapper;
    private final LiteratureDocumentConverter documentConverter;

    public LiteratureLoadProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
        this.documentConverter = new LiteratureDocumentConverter();
    }

    @Override
    public LiteratureDocument process(LiteratureEntry entry) throws Exception {
        LiteratureEntryBuilder entryBuilder = LiteratureEntryBuilder.from(entry);
        Citation literature = entry.getCitation();
        SolrQuery query = new SolrQuery("id:" + literature.getId());
        Optional<LiteratureDocument> optionalDocument =
                uniProtSolrClient.queryForObject(
                        SolrCollection.literature, query, LiteratureDocument.class);
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            // Get statistics and mapped references from previous steps and copy it to entry builder
            byte[] literatureObj = document.getLiteratureObj();
            LiteratureEntry existingEntry =
                    literatureObjectMapper.readValue(literatureObj, LiteratureEntryImpl.class);
            entryBuilder.statistics(existingEntry.getStatistics());
        }
        return documentConverter.convert(entryBuilder.build());
    }
}
