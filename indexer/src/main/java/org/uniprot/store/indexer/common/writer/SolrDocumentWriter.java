package org.uniprot.store.indexer.common.writer;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.Document;

/** @author lgonzales */
@Slf4j
public class SolrDocumentWriter<T extends Document> implements ItemWriter<T> {
    private final UniProtSolrOperations solrOperations;
    private final SolrCollection collection;

    public SolrDocumentWriter(UniProtSolrOperations solrOperations, SolrCollection collection) {
        this.solrOperations = solrOperations;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends T> items) {
        try {
            this.solrOperations.saveBeans(collection.name(), items);
            this.solrOperations.softCommit(collection.name());
        } catch (Throwable error) {
            log.error("Error writing to solr: ", error);
            String ids =
                    items.stream().map(Document::getDocumentId).collect(Collectors.joining(", "));
            log.warn("Failed document ids: " + ids);
            throw error;
        }
    }
}
