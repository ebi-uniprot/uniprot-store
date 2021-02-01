package org.uniprot.store.indexer.genecentric;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;

/**
 * @author jluo
 * @date: 16 May 2019
 */
public class GeneCentricCanonicalWriter implements ItemWriter<GeneCentricDocument> {
    private final UniProtSolrClient uniProtSolrClient;
    private final SolrCollection collection;

    public GeneCentricCanonicalWriter(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.collection = SolrCollection.genecentric;
    }

    @Override
    public void write(List<? extends GeneCentricDocument> documents) throws Exception {
        this.uniProtSolrClient.saveBeans(collection, documents);
    }
}
