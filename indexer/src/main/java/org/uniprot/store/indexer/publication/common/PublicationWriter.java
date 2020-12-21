package org.uniprot.store.indexer.publication.common;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.Document;
import org.uniprot.store.search.document.publication.PublicationDocument;

/** @author Edd */
@Slf4j
public class PublicationWriter implements ItemWriter<PublicationDocument> {
    private final UniProtSolrClient uniProtSolrClient;

    public PublicationWriter(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Override
    public void write(List<? extends PublicationDocument> list) throws Exception {
        StringBuilder deleteQueryBuilder = new StringBuilder();
        boolean first = true;
        try {
            for (PublicationDocument doc : list) {
                if (!first) {
                    deleteQueryBuilder.append(" OR ");
                }
                deleteQueryBuilder.append("id:").append(doc.getDocumentId());
                first = false;
            }

            uniProtSolrClient.delete(SolrCollection.publication, deleteQueryBuilder.toString());
            uniProtSolrClient.saveBeans(SolrCollection.publication, list);
            uniProtSolrClient.softCommit(SolrCollection.publication);
        } catch (Exception error) {
            log.error("Error writing to solr: ", error);
            String ids =
                    list.stream().map(Document::getDocumentId).collect(Collectors.joining(", "));
            log.warn("Failed to write document ids: " + ids);
            throw error;
        }
    }
}
