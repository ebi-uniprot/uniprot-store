package org.uniprot.store.indexer.publication.uniprotkb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.publication.impl.MappedPublicationsBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryReferencesConverter;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @created 16/12/2020
 */
public class UniProtPublicationProcessor
        implements ItemProcessor<UniProtEntryDocumentPair, List<PublicationDocument>> {

    private UniProtEntryReferencesConverter converter;
    private UniProtSolrClient uniProtSolrClient;
    private SolrCollection collection;
    private ObjectMapper objectMapper;

    public UniProtPublicationProcessor(
            UniProtSolrClient uniProtSolrClient, SolrCollection collection) {
        this.converter = new UniProtEntryReferencesConverter();
        this.uniProtSolrClient = uniProtSolrClient;
        this.collection = collection;
        this.objectMapper = MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public List<PublicationDocument> process(UniProtEntryDocumentPair item) throws Exception {
        /*
        case 1 - If pubmedid is there, try to get documents by pubmedid and accession then upsert
        case 2 - If pumedid is missing, then insert, no need to check in solr because we don't need to update such docs
        and can insert such docs
        */
        List<PublicationDocument> updatedDocuments = new ArrayList<>();
        List<PublicationDocument> uniprotPubDocs =
                this.converter.convertToPublicationDocuments(item.getEntry());
        for (PublicationDocument currentDoc : uniprotPubDocs) {
            if (Objects.isNull(currentDoc.getPubMedId())) { // case 2 - read above
                updatedDocuments.add(currentDoc);
            } else { // case 1 - read above
                SolrQuery query = getSearchQuery(currentDoc);
                List<PublicationDocument> existingDocs =
                        uniProtSolrClient.query(collection, query, PublicationDocument.class);
                populateUpdatedDocuments(currentDoc, existingDocs, updatedDocuments);
            }
        }
        return updatedDocuments;
    }

    private SolrQuery getSearchQuery(PublicationDocument currentDoc) {
        return new SolrQuery(
                "pubmed_id:"
                        + currentDoc.getPubMedId()
                        + " AND accession:"
                        + currentDoc.getAccession());
    }

    private void populateUpdatedDocuments(
            PublicationDocument currentDoc,
            List<PublicationDocument> existingDocs,
            List<PublicationDocument> updatedDocuments)
            throws IOException {
        if (existingDocs.isEmpty()) {
            updatedDocuments.add(currentDoc);
        } else { // update binary MappedPublications object, types and categories
            for (PublicationDocument existingDoc : existingDocs) {
                PublicationDocument updatedDoc = mergeDocuments(existingDoc, currentDoc);
                updatedDocuments.add(updatedDoc);
            }
        }
    }

    private PublicationDocument mergeDocuments(
            PublicationDocument existingDoc, PublicationDocument currentDoc) throws IOException {
        MappedPublications mergedMappedPubs = mergeMappedPublications(existingDoc, currentDoc);

        PublicationDocument.PublicationDocumentBuilder builder = PublicationDocument.builder();
        builder.id(existingDoc.getId()).pubMedId(currentDoc.getPubMedId());
        Set<String> cats = existingDoc.getCategories();
        cats.addAll(currentDoc.getCategories());
        builder.categories(cats);
        Set<Integer> types = existingDoc.getTypes();
        types.addAll(currentDoc.getTypes());
        builder.types(types);
        builder.computationalMappedProteinCount(existingDoc.getComputationalMappedProteinCount());
        builder.communityMappedProteinCount(existingDoc.getCommunityMappedProteinCount());
        builder.unreviewedMappedProteinCount(existingDoc.getUnreviewedMappedProteinCount());
        builder.reviewedMappedProteinCount(existingDoc.getReviewedMappedProteinCount());
        builder.isLargeScale(existingDoc.isLargeScale());
        builder.publicationMappedReferences(
                converter.getMappedPublicationsBinary(mergedMappedPubs));
        return builder.build();
    }

    private MappedPublications mergeMappedPublications(
            PublicationDocument existingDoc, PublicationDocument currentDoc) throws IOException {
        MappedPublications currentMappedPubs =
                objectMapper.readValue(
                        currentDoc.getPublicationMappedReferences(), MappedPublications.class);
        MappedPublications existingMappedPubs =
                objectMapper.readValue(
                        existingDoc.getPublicationMappedReferences(), MappedPublications.class);
        MappedPublicationsBuilder builder = MappedPublicationsBuilder.from(existingMappedPubs);
        int type = new ArrayList<>(currentDoc.getTypes()).get(0);
        if (MappedReferenceType.UNIPROTKB_REVIEWED.getIntValue() == type) {
            builder.reviewedMappedReference(currentMappedPubs.getReviewedMappedReference());
        } else {
            builder.reviewedMappedReference(currentMappedPubs.getUnreviewedMappedReference());
        }
        return builder.build();
    }
}
