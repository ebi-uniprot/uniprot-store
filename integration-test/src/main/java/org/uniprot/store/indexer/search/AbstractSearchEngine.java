package org.uniprot.store.indexer.search;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.search.document.Document;
import org.uniprot.store.search.document.DocumentConverter;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * Class that sets up a search engine for testing purposes.
 *
 * <p>Uses an {@link org.apache.solr.client.solrj.embedded.EmbeddedSolrServer} to create a
 * standalone solr instance
 *
 * <p>The class defines methods to insert/delete/query data from the data source.
 */
public abstract class AbstractSearchEngine<E> implements BeforeAllCallback, AfterAllCallback {
    private static final String SOLR_CONFIG_DIR =
            "../index-config/src/main/solr-config/uniprot-collections";
    private static final Logger logger = LoggerFactory.getLogger(AbstractSearchEngine.class);

    private static final int DEFAULT_RETRIEVABLE_ROWS = 1000;
    protected final File indexHome;
    private final String searchEngineName;
    private SolrClient server;
    private DocumentConverter<E, ?> documentConverter;

    private int retrievableRows;

    public AbstractSearchEngine(
            String searchEngineName, DocumentConverter<E, ?> documentConverter) {
        this.documentConverter = documentConverter;
        this.searchEngineName = searchEngineName;

        this.retrievableRows = DEFAULT_RETRIEVABLE_ROWS;

        this.indexHome = Files.createTempDir();
    }

    public void indexEntry(E entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry is null");
        }

        Document document = documentConverter.convert(entry);

        saveDocument(document);
    }

    protected void saveDocument(Document document) {
        try {
            server.addBean(document);
            server.commit();
        } catch (SolrServerException | IOException e) {
            throw new IllegalStateException("Problem indexing document.", e);
        }
    }

    public void indexEntry(E entry, DocFieldTransformer docTransformer) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry is null");
        }

        Document document = documentConverter.convert(entry);
        docTransformer.accept(document);

        saveDocument(document);
    }

    public void removeEntry(String entryId) {
        String idQuery = identifierQuery(entryId);

        QueryResponse queryResponse = getQueryResponse(idQuery);
        if (queryResponse.getResults() != null && !queryResponse.getResults().isEmpty()) {
            try {
                server.deleteByQuery(idQuery);
                server.commit();
            } catch (SolrServerException | IOException e) {
                throw new IllegalStateException(
                        "Failed to remove entry with id: " + entryId + " from index.", e);
            }
        }
    }

    public QueryResponse getQueryResponse(String query) {
        SolrQuery solrQuery = new SolrQuery(query);

        return getQueryResponse(solrQuery);
    }

    public QueryResponse getQueryResponse(String requestHandler, String query) {
        SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setRequestHandler(requestHandler);

        return getQueryResponse(solrQuery);
    }

    public QueryResponse getQueryResponse(SolrQuery query) {
        try {
            return server.query(query);
        } catch (SolrServerException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<String> getIdentifiers(QueryResponse response) {
        List<String> accessions = new ArrayList<>();

        SolrDocumentList documents = response.getResults();

        for (SolrDocument document : documents) {
            String accession = (String) document.get(identifierField());

            accessions.add(accession);
        }

        return accessions;
    }

    public void printIndexContents() {
        // show all results
        SolrQuery allQuery = new SolrQuery("*:*");
        allQuery.setRows(retrievableRows);

        SolrDocumentList results = null;
        QueryResponse queryResponse;

        try {
            queryResponse = server.query(allQuery);
            results = queryResponse.getResults();
        } catch (SolrServerException | IOException e) {
            logger.error("Failed query: ", e);
        }

        if (results != null) {
            logger.debug("Index contents ({}) start ---------", results.size());
            for (SolrDocument solrDocument : results) {
                logger.debug("index contains: {}", solrDocument);
            }
        }

        logger.debug("Index contents end ---------");
    }

    protected abstract String identifierQuery(String entryId);

    protected abstract String identifierField();

    protected abstract SearchFieldConfig getSearchFieldConfig();

    @Override
    public void beforeAll(ExtensionContext context) {
        createServer();
    }

    private void createServer() {
        // properties used by solrconfig.xml files in the cores' conf directories
        System.setProperty("solr.data.home", indexHome.getAbsolutePath() + "/solr/data");
        System.setProperty("solr.core.name", "uniprot");
        System.setProperty("solr.ulog.dir", indexHome.getAbsolutePath() + "/tlog");

        Path solrConfigDir = Paths.get(SOLR_CONFIG_DIR);

        logger.info(solrConfigDir.toString());
        CoreContainer container = new CoreContainer(solrConfigDir, new Properties());

        container.load();
        container.waitForLoadingCoresToFinish((long) 60 * 1000);

        if (!container.isLoaded(searchEngineName)) {
            container
                    .getCoreInitFailures()
                    .forEach(
                            (key, value) ->
                                    logger.error(
                                            "Search engine " + key + ", has failed: ",
                                            value.exception));
            throw new IllegalStateException(
                    "Search engine " + searchEngineName + ", has not loaded properly");
        }

        server = new EmbeddedSolrServer(container, searchEngineName);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        closeServer();
    }

    private void closeServer() {
        try {
            server.close();
            indexHome.deleteOnExit();
            logger.debug("cleaned up solr home in ({}) now that the tests are finished", indexHome);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void setMaxRetrievableRows(int rows) {
        Preconditions.checkArgument(rows > 0, "Provided row value is negative: " + rows);
        retrievableRows = rows;
    }
}
