package uk.ac.ebi.uniprot.indexer;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.mockito.stubbing.Stubber;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;

import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

/**
 * Utility methods to help testing the retry logic associated with writing
 * documents to an index, in the presence of a possibly problematic Solr instance,
 * which may occasionally not be responsive due to errors, e.g., from network issues.
 * <p>
 * Created 12/04/19
 *
 * @author Edd
 */
public class DocumentWriteRetryHelper {
    private static final String HOST = "http://www.myhost.com";
    private static final String MESSAGE = "Looks like the host is not reachable?!";
    private static final int CODE = 1;

    /**
     * Stubs successive {@link UniProtSolrOperations#saveBeans(String, Collection)} calls, based on a given list of
     * {@link SolrResponse} values. {@link SolrResponse#OK} simulates that Solr was able to write the documents it
     * received; {@link SolrResponse#REMOTE_EXCEPTION} simulates Solr being busy and responding
     * with a {@link HttpSolrClient.RemoteSolrException}, meaning the documents could not be
     * written
     *
     * @param responses represents a list of behavioural responses from Solr
     * @return a {@link Stubber} which can be associated with a method call
     */
    public static Stubber stubSolrWriteResponses(List<SolrResponse> responses) {
        Stubber stubber = null;
        for (SolrResponse response : responses) {
            switch (response) {
                case OK:
                    stubber = (stubber == null) ? doReturn(getOKResponse()) : stubber.doReturn(getOKResponse());
                    break;
                case REMOTE_EXCEPTION:
                    stubber = (stubber == null) ? doThrow(new HttpSolrClient.RemoteSolrException(HOST, CODE, MESSAGE, null))
                            : stubber.doThrow(new HttpSolrClient.RemoteSolrException(HOST, CODE, MESSAGE, null));
                    break;
                default:
                    throw new IllegalStateException("Unknown SolrResponse");
            }
        }
        return stubber;
    }

    private static UpdateResponse getOKResponse() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }
        return new UpdateResponse();
    }

    /**
     * Represents possible responses from Solr -- extend if needed
     */
    public enum SolrResponse {
        OK, REMOTE_EXCEPTION
    }
}
