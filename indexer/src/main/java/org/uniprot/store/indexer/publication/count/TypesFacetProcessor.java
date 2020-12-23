package org.uniprot.store.indexer.publication.count;

import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * @author sahmad
 * @created 23/12/2020
 */
public class TypesFacetProcessor implements ItemProcessor<LiteratureDocument, PublicationDocument> {
    private UniProtSolrClient solrClient;
    private static final String TYPES_SOLR_FIELD = "types";
    static final String PUBMED_ID_SOLR_FIELD = "pubmed_id";
    static final String COLON_STR = ":";
    private static final int MIN_SCALE_MAX = 50;

    public TypesFacetProcessor(UniProtSolrClient solrClient) {
        this.solrClient = solrClient;
    }

    @Override
    public PublicationDocument process(LiteratureDocument item) throws Exception {
        SolrQuery query = getFacetQuery(item);
        QueryResponse response = this.solrClient.query(SolrCollection.publication, query);
        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();
        builder.pubMedId(item.getId());
        List<FacetField.Count> facetValues = response.getFacetField(TYPES_SOLR_FIELD).getValues();
        if (Utils.notNullNotEmpty(facetValues)) {
            setProteinCounts(builder, facetValues);
            builder.isLargeScale(isLargeScale(builder.build()));
            PublicationDocument docsWithStats = builder.build();
            return docsWithStats;
        }
        return null;
    }

    private SolrQuery getFacetQuery(LiteratureDocument item) {
        SolrQuery query = new SolrQuery(PUBMED_ID_SOLR_FIELD + COLON_STR + item.getId());
        query.addFacetField(TYPES_SOLR_FIELD);
        return query;
    }

    private void setProteinCounts(
            PublicationDocument.PublicationDocumentBuilder builder,
            List<FacetField.Count> facetValues) {
        facetValues.stream().forEach(fv -> setProteinCount(builder, fv));
    }

    private void setProteinCount(
            PublicationDocument.PublicationDocumentBuilder builder, FacetField.Count facetValue) {
        MappedReferenceType type =
                MappedReferenceType.getType(Integer.parseInt(facetValue.getName()));
        switch (type) {
            case UNIPROTKB_REVIEWED:
                builder.reviewedMappedProteinCount(facetValue.getCount());
                break;
            case UNIPROTKB_UNREVIEWED:
                builder.unreviewedMappedProteinCount(facetValue.getCount());
                break;
            case COMMUNITY:
                builder.communityMappedProteinCount(facetValue.getCount());
                break;
            case COMPUTATIONAL:
                builder.computationalMappedProteinCount(facetValue.getCount());
                break;
            default:
                throw new UnsupportedOperationException("type is not supported : " + type);
        }
    }

    private boolean isLargeScale(PublicationDocument document) {
        long rvCount =
                document.getReviewedMappedProteinCount() == null
                        ? 0
                        : document.getReviewedMappedProteinCount();
        long unrvCount =
                document.getUnreviewedMappedProteinCount() == null
                        ? 0
                        : document.getUnreviewedMappedProteinCount();
        long commCount =
                document.getCommunityMappedProteinCount() == null
                        ? 0
                        : document.getCommunityMappedProteinCount();
        long computCount =
                document.getComputationalMappedProteinCount() == null
                        ? 0
                        : document.getComputationalMappedProteinCount();
        return rvCount + unrvCount + commCount + computCount > MIN_SCALE_MAX;
    }
}
