package org.uniprot.store.indexer.publication.common;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CursorMarkParams;
import org.springframework.batch.item.ItemReader;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 13/01/2021
 */
@Slf4j
public class LargeScaleReader implements ItemReader<Set<String>> {

    private final UniProtSolrClient uniProtSolrClient;
    private final LargeScaleSolrFieldQuery queryField;
    private boolean hasFinishedRead = false;

    public LargeScaleReader(
            UniProtSolrClient uniProtSolrClient, LargeScaleSolrFieldQuery queryField) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.queryField = queryField;
    }

    @Override
    public Set<String> read() {
        Set<String> result = new HashSet<>();
        SolrQuery solrQuery = new SolrQuery(queryField.getSolrFieldNameQuery());
        solrQuery.setRows(200);
        solrQuery.setSort("id", SolrQuery.ORDER.desc);
        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean hasMoreSolrData = true;
        while (hasMoreSolrData && !hasFinishedRead) {
            solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
            QueryResponse response = uniProtSolrClient.query(SolrCollection.literature, solrQuery);
            String nextCursorMark = response.getNextCursorMark();

            response.getBeans(LiteratureDocument.class).stream()
                    .map(this::getLiteratureBinary)
                    .filter(LiteratureEntry::hasStatistics)
                    .filter(entry -> entry.getStatistics().isLargeScale())
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
                hasMoreSolrData = false;
            }
            cursorMark = nextCursorMark;
        }
        if (result.isEmpty()) {
            result = null;
        } else {
            hasFinishedRead = true;
            log.info("Large scale pubmedIds count: " + result.size());
        }
        return result;
    }

    public LiteratureEntry getLiteratureBinary(LiteratureDocument literatureDocument) {
        LiteratureEntry result = null;
        try {
            result =
                    LiteratureJsonConfig.getInstance()
                            .getFullObjectMapper()
                            .readValue(
                                    literatureDocument.getLiteratureObj(), LiteratureEntry.class);
        } catch (Exception e) {
            throw new DocumentConversionException(
                    "Unable to parse LiteratureEntry to binary json: ", e);
        }
        return result;
    }
}
