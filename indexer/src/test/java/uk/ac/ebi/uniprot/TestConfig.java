/*
 * Created by sahmad on 07/02/19 16:44
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.core.CoreContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import uk.ac.ebi.uniprot.writers.DBXRefWriter;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

@TestConfiguration
public class TestConfig {
    private static final String DBXREF_COLLECTION_NAME = "crossref";
    private static final String SOLR_HOME = "target/test-classes/solr-config/uniprot-collections";
    @Bean
    public SolrClient solrClient(){
        CoreContainer container = new CoreContainer(new File(SOLR_HOME).getAbsolutePath());
        container.load();
        SolrClient solrClient = new EmbeddedSolrServer(container, DBXREF_COLLECTION_NAME);
        return solrClient;
    }
}
