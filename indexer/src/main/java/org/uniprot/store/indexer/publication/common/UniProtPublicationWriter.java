package org.uniprot.store.indexer.publication.common;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.Document;
import org.uniprot.store.search.document.publication.PublicationDocument;

import lombok.extern.slf4j.Slf4j;

/**
 * @author sahmad
 * @created 16/12/2020
 */
@Slf4j
public class UniProtPublicationWriter implements ItemWriter<List<PublicationDocument>> {
    private UniProtSolrClient uniProtSolrClient;

    public UniProtPublicationWriter(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Override
    public void write(List<? extends List<PublicationDocument>> items) throws Exception {
        try {
            List<PublicationDocument> flattenItems =
                    items.stream().flatMap(Collection::stream).collect(Collectors.toList());
            if (!flattenItems.isEmpty()) {
                this.uniProtSolrClient.saveBeans(SolrCollection.publication, flattenItems);
            }
        } catch (Exception error) {
            log.error("Error writing to solr: ", error);
            String ids =
                    items.stream()
                            .flatMap(Collection::stream)
                            .map(Document::getDocumentId)
                            .collect(Collectors.joining(", "));
            log.warn("Failed document ids: " + ids);
            throw error;
        }
    }
}
