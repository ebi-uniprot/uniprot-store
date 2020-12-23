package org.uniprot.store.indexer.publication.count;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * @author sahmad
 * @created 23/12/2020
 */
public class LiteratureDocumentReader implements ItemReader<LiteratureDocument> {
    private static final String ID_SOLR_FIELD = "id";
    private UniProtSolrClient solrClient;
    private List<LiteratureDocument> literatureDocs;
    private Long totalDocuments;
    private Long current;
    private int batchSize;

    public LiteratureDocumentReader(UniProtSolrClient solrClient) {
        this.solrClient = solrClient;
        this.literatureDocs = new ArrayList<>();
        this.current = 0L;
        this.batchSize = 100;
    }

    @Override
    public LiteratureDocument read()
            throws Exception, UnexpectedInputException, ParseException,
                    NonTransientResourceException {
        if (this.literatureDocs.isEmpty()) {
            populateLiteratureDocuments();
        }
        for (Iterator<LiteratureDocument> iterator = literatureDocs.listIterator();
                iterator.hasNext(); ) {
            LiteratureDocument currentDoc = iterator.next();
            iterator.remove();
            return currentDoc;
        }
        return null;
    }

    private void populateLiteratureDocuments() {
        if (Objects.isNull(this.totalDocuments) || this.current < this.totalDocuments) {
            SolrQuery query = new SolrQuery("*:*");
            query.addSort(ID_SOLR_FIELD, SolrQuery.ORDER.asc);
            query.setStart(Math.toIntExact(this.current));
            query.setRows(this.batchSize);
            QueryResponse response = this.solrClient.query(SolrCollection.literature, query);
            if (this.totalDocuments == null) { // set total record count just once
                this.totalDocuments = response.getResults().getNumFound();
            }
            List<LiteratureDocument> currentBatchDocs = response.getBeans(LiteratureDocument.class);
            this.literatureDocs.addAll(currentBatchDocs);
            this.current = this.current + currentBatchDocs.size();
        }
    }
}
