package org.uniprot.store.indexer;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.uniprot.store.search.SolrCollection;

import java.io.IOException;

/**
 * Created 19/09/18
 *
 * @author Edd
 */
public class ClosableEmbeddedSolrClient extends SolrClient {
    public static final String SOLR_HOME = "solr.home";
    private final EmbeddedSolrServer server;

    public ClosableEmbeddedSolrClient(CoreContainer container, SolrCollection collection) {
        this.server = new EmbeddedSolrServer(container, collection.name());
    }

    @Override
    public NamedList<Object> request(SolrRequest solrRequest, String s) throws SolrServerException, IOException {
        return server.request(solrRequest, s);
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
}
