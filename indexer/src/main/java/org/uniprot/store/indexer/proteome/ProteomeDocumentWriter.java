package org.uniprot.store.indexer.proteome;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

/**
 * @author jluo
 * @date: 17 May 2019
 */
public class ProteomeDocumentWriter implements ItemWriter<Proteome> {
    private final ItemProcessor<Proteome, ProteomeDocument> itemProcessor;
    private final UniProtSolrClient solrClient;
    private final SolrCollection collection;

    public ProteomeDocumentWriter(
            ItemProcessor<Proteome, ProteomeDocument> itemProcessor, UniProtSolrClient solrClient) {
        this.itemProcessor = itemProcessor;
        this.solrClient = solrClient;
        this.collection = SolrCollection.proteome;
    }

    @Override
    public void write(List<? extends Proteome> items) throws Exception {
        List<ProteomeDocument> documents = new ArrayList<>();
        for (Proteome proteome : items) {
            documents.add(itemProcessor.process(proteome));
        }
        solrClient.saveBeans(collection, documents);
        solrClient.softCommit(collection);
    }
}
