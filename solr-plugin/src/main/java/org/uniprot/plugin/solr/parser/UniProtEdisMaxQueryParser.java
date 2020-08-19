package org.uniprot.plugin.solr.parser;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedDismaxQParser;
import org.apache.solr.search.SyntaxError;

/**
 * Created 18/08/2020
 *
 * @author Edd
 */
class UniProtEdisMaxQueryParser extends ExtendedDismaxQParser {
    private final QueryTransformer queryTransformer;

    public UniProtEdisMaxQueryParser(
            String qstr,
            SolrParams localParams,
            SolrParams params,
            SolrQueryRequest req,
            QueryTransformer queryTransformer) {
        super(qstr, localParams, params, req);
        this.queryTransformer = queryTransformer;
    }

    @Override
    public Query parse() throws SyntaxError {
        Query query = super.parse();
        queryTransformer.transform(query);
        return query;
    }
}
