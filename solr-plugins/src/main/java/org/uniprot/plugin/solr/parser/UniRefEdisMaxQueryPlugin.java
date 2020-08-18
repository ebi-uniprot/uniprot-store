package org.uniprot.plugin.solr.parser;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;

/**
 * Created 18/08/2020
 *
 * @author Edd
 */
public class UniRefEdisMaxQueryPlugin extends QParserPlugin {
    @Override
    public QParser createParser(
            String qstr,
            SolrParams localParams,
            SolrParams solrParams,
            SolrQueryRequest solrQueryRequest) {
        QueryTransformer queryTransformer = init();
        return new UniProtEdisMaxQueryParser(
                qstr, localParams, solrParams, solrQueryRequest, queryTransformer);
    }

    private QueryTransformer init() {
        return QueryTransformer.builder()
                .queryTransformer(
                        Optimiser.builder()
                                .optimisedField(
                                        SearchFieldConfigFactory.getSearchFieldConfig(
                                                        UniProtDataType.UNIREF)
                                                .getSearchFieldItemByName("id"))
                                .build())
                .build();
    }
}
