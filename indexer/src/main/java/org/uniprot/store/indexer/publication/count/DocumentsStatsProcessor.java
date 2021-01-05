package org.uniprot.store.indexer.publication.count;

import static org.uniprot.store.indexer.publication.count.TypesFacetProcessor.COLON_STR;
import static org.uniprot.store.indexer.publication.count.TypesFacetProcessor.PUBMED_ID_SOLR_FIELD;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * @author sahmad
 * @created 23/12/2020
 */
public class DocumentsStatsProcessor
        implements ItemProcessor<PublicationDocument, List<PublicationDocument>> {
    private UniProtSolrClient solrClient;

    public DocumentsStatsProcessor(UniProtSolrClient solrClient) {
        this.solrClient = solrClient;
    }

    @Override
    public List<PublicationDocument> process(PublicationDocument modelDocWithStats)
            throws Exception {
        SolrQuery query = getAllQuery(modelDocWithStats);
        List<PublicationDocument> documents =
                this.solrClient.query(SolrCollection.publication, query, PublicationDocument.class);
        List<PublicationDocument> documentsWithStats =
                updateDocumentsWithStats(modelDocWithStats, documents);
        return documentsWithStats;
    }

    private List<PublicationDocument> updateDocumentsWithStats(
            PublicationDocument modelDocWithStats, List<PublicationDocument> documents) {
        List<PublicationDocument> documentsWithStats = new ArrayList<>();
        for (PublicationDocument document :
                documents) { // update stats from modelDocWithStats for each document
            PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();
            builder.id(document.getId()).accession(document.getAccession());
            builder.pubMedId(modelDocWithStats.getPubMedId());
            builder.categories(document.getCategories());
            builder.types(document.getTypes());
            builder.mainType(document.getMainType());
            builder.refNumber(document.getRefNumber());
            builder.reviewedMappedProteinCount(modelDocWithStats.getReviewedMappedProteinCount());
            builder.unreviewedMappedProteinCount(
                    modelDocWithStats.getUnreviewedMappedProteinCount());
            builder.communityMappedProteinCount(modelDocWithStats.getCommunityMappedProteinCount());
            builder.computationalMappedProteinCount(
                    modelDocWithStats.getComputationalMappedProteinCount());
            builder.isLargeScale(modelDocWithStats.isLargeScale());
            builder.publicationMappedReferences(document.getPublicationMappedReferences());
            documentsWithStats.add(builder.build());
        }
        return documentsWithStats;
    }

    private SolrQuery getAllQuery(PublicationDocument docWithStats) {
        SolrQuery query =
                new SolrQuery(PUBMED_ID_SOLR_FIELD + COLON_STR + docWithStats.getPubMedId());
        query.setRows(50000); // TODO what if more than 50k
        return query;
    }
}
