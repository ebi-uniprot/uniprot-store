package org.uniprot.store.indexer.proteome;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

/**
 * @author jluo
 * @date: 17 May 2019
 */
public class ProteomeDocumentWriter implements ItemWriter<ProteomeDocument> {
    private final UniProtSolrClient uniProtSolrClient;
    private final SolrCollection collection;

    public ProteomeDocumentWriter(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.collection = SolrCollection.proteome;
    }

    @Override
    public void write(List<? extends ProteomeDocument> documents) throws Exception {
        uniProtSolrClient.saveBeans(collection, documents);
        uniProtSolrClient.softCommit(collection);
    }
}
