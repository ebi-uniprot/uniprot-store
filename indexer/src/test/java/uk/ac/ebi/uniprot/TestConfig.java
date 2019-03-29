/*
 * Created by sahmad on 07/02/19 16:44
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot;

import org.apache.solr.client.solrj.SolrClient;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import uk.ac.ebi.uniprot.api.common.repository.search.ClosableEmbeddedSolrClient;
import uk.ac.ebi.uniprot.api.common.repository.search.SolrCollection;

@TestConfiguration
public class TestConfig {

    @Bean
    public SolrClient solrClient() {
        ClosableEmbeddedSolrClient solrClient = new ClosableEmbeddedSolrClient(SolrCollection.crossref);
        return solrClient;
    }
}
