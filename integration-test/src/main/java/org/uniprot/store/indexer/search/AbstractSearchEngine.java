package org.uniprot.store.indexer.search;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.store.indexer.converter.DocumentConverter;
import org.uniprot.store.search.document.Document;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Class that sets up a search engine for testing purposes.
 * <p>
 * Uses an {@link org.apache.solr.client.solrj.embedded.EmbeddedSolrServer} to create a standalone solr instance
 * <p>
 * The class defines methods to insert/delete/query data from the data source.
 */
public abstract class AbstractSearchEngine<E> extends ExternalResource {
    private static final String SOLR_CONFIG_DIR = "../index-config/src/main/solr-config/uniprot-collections";
    private static final Logger logger = LoggerFactory.getLogger(AbstractSearchEngine.class);

    private static final int DEFAULT_RETRIEVABLE_ROWS = 1000;
    protected final File indexHome;
    private final String searchEngineName;
    private SolrClient server;
    private DocumentConverter<E, ?> documentConverter;

    private int retrievableRows;

    public AbstractSearchEngine(String searchEngineName, DocumentConverter<E, ?> documentConverter) {
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

        try {
            server.addBean(document);
            server.commit();
        } catch (SolrServerException | IOException e) {
            throw new IllegalStateException("Problem indexing document.", e);
        }
    }

    public void removeEntry(String entryId) {
        String idQuery = identifierQuery(entryId);

        QueryResponse queryResponse = getQueryResponse(idQuery);
        if (queryResponse.getResults() != null && !queryResponse.getResults().isEmpty()) {
            try {
                server.deleteByQuery(idQuery);
                server.commit();
            } catch (SolrServerException | IOException e) {
                throw new IllegalStateException("Failed to remove entry with id: " + entryId + " from index.", e);
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
            String accession = (String) document.get(identifierField().name());

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
        } catch (SolrServerException e) {
            logger.error("Failed query: ", e);
        } catch (IOException e) {
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

    protected abstract Enum identifierField();

    @Override
    protected void before() throws Throwable {
        // properties used by solrconfig.xml files in the cores' conf directories
        System.setProperty("solr.data.dir", indexHome.getAbsolutePath() + "/solr/data");
        System.setProperty("solr.core.name", "uniprot");
        System.setProperty("solr.ulog.dir", indexHome.getAbsolutePath() + "/tlog");
        //   System.setProperty("solr.lib.ujdk.location", ".");
//        System.setProperty("uniprot.voldemort.url", "inMemoryStore");
//        System.setProperty("uniprot.voldemort.store", "avro-uniprot");
//        System.setProperty("uniparc.voldemort.url", "inMemoryStore");
//        System.setProperty("uniparc.voldemort.store", "avro-uniparc");

        File solrConfigDir = new File(SOLR_CONFIG_DIR);

        System.out.println(solrConfigDir.getAbsolutePath());
        CoreContainer container = new CoreContainer(solrConfigDir.getAbsolutePath());

        container.load();


        if (!container.isLoaded(searchEngineName)) {
            container.getCoreInitFailures().forEach((key, value) -> {
                logger.error("Search engine " + key + ", has failed: ", value.exception);
            });
            throw new IllegalStateException("Search engine " + searchEngineName + ", has not loaded properly");
        }

        server = new EmbeddedSolrServer(container, searchEngineName);
    }

    @Override
    protected void after() {
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