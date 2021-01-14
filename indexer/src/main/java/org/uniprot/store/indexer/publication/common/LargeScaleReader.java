package org.uniprot.store.indexer.publication.common;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CursorMarkParams;
import org.springframework.batch.item.ItemReader;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * @author lgonzales
 * @since 13/01/2021
 */
@Slf4j
public class LargeScaleReader implements ItemReader<Set<String>> {

    private final UniProtSolrClient uniProtSolrClient;
    private final LargeScaleSolrFieldName queryField;
    private boolean hasFinished = false;

    public LargeScaleReader(
            UniProtSolrClient uniProtSolrClient, LargeScaleSolrFieldName queryField) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.queryField = queryField;
    }

    @Override
    public Set<String> read() {
        Set<String> result = new HashSet<>();
        SolrQuery solrQuery = new SolrQuery(queryField.getSolrFieldName() + ":true");
        solrQuery.setRows(200);
        solrQuery.setSort("id", SolrQuery.ORDER.desc);
        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean done = false;
        while (!done && !hasFinished) {
            solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
            QueryResponse response = uniProtSolrClient.query(SolrCollection.literature, solrQuery);
            String nextCursorMark = response.getNextCursorMark();

            response.getBeans(LiteratureDocument.class).stream()
                    .map(this::getLiteratureBinary)
                    .filter(LiteratureEntry::hasStatistics)
                    .filter(entry -> isLargeScale(entry.getStatistics()))
                    .map(LiteratureEntry::getCitation)
                    .map(
                            citation ->
                                    citation.getCitationCrossReferenceByType(
                                            CitationDatabase.PUBMED))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(CrossReference::getId)
                    .forEach(result::add);

            if (cursorMark.equals(nextCursorMark)) {
                done = true;
            }
            cursorMark = nextCursorMark;
        }
        if(result.isEmpty()){
            result = null;
        } else {
            hasFinished = true;
            log.info("Large scale pubmedIds count: " + result.size());
        }
        return result;
    }

    private boolean isLargeScale(LiteratureStatistics statistics) {
        return (statistics.getCommunityMappedProteinCount()
                        + statistics.getCommunityMappedProteinCount()
                        + statistics.getReviewedProteinCount()
                        + statistics.getUnreviewedProteinCount())
                > 50;
    }

    public LiteratureEntry getLiteratureBinary(LiteratureDocument literatureDocument) {
        LiteratureEntry result = null;
        try {
            result =
                    LiteratureJsonConfig.getInstance()
                            .getFullObjectMapper()
                            .readValue(
                                    literatureDocument.getLiteratureObj().array(),
                                    LiteratureEntry.class);
        } catch (Exception e) {
            throw new DocumentConversionException(
                    "Unable to parse LiteratureEntry to binary json: ", e);
        }
        return result;
    }
}
