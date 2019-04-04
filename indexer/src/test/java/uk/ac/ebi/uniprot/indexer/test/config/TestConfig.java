/*
 * Created by sahmad on 07/02/19 16:44
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.indexer.test.config;

import org.apache.solr.client.solrj.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.server.support.EmbeddedSolrServerFactory;

import java.io.File;
import java.nio.file.Files;

@TestConfiguration
public class TestConfig implements DisposableBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestConfig.class);
    private final File file;

    public TestConfig() throws Exception{
        file = Files.createTempDirectory("solrhome").toFile();
    }

    @Value(("${solr.home}"))
    private String solrHome;

    @Bean
    @Profile("offline")
    public SolrClient solrClient() throws Exception {
        System.setProperty("solr.data.dir", file.getAbsolutePath());
        EmbeddedSolrServerFactory factory = new EmbeddedSolrServerFactory(solrHome);
        SolrClient solrClient = factory.getSolrClient();
        return solrClient;
    }

    @Bean
    @Profile("offline")
    public SolrTemplate solrTemplate(SolrClient solrClient) {
        return new SolrTemplate(solrClient);
    }

    @Override
    public void destroy() throws Exception {
        if(file != null) {
            boolean deleted = file.delete();
            if(deleted){
                LOGGER.info("deleted solr home");
            }else{
                LOGGER.warn("NOT deleted solr home");
            }
        }
    }
}
