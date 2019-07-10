/*
 * Created by sahmad on 07/02/19 16:44
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.indexer.test.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.data.solr.core.SolrOperations;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.server.support.EmbeddedSolrServerFactory;

import java.io.File;
import java.nio.file.Files;

import static org.mockito.Mockito.mock;

@TestConfiguration
@Slf4j
public class SolrTestConfig implements DisposableBean {
    private static final String SOLR_DATA_DIR = "solr.data.dir";
    private static final String TEMP_DIR_PREFIX = "test-solr-data-dir";
    private final File file;

    @Value(("${solr.home}"))
    private String solrHome;

    public SolrTestConfig() throws Exception {
        file = Files.createTempDirectory(TEMP_DIR_PREFIX).toFile();
    }

    @Bean
    @Profile("offline")
    public HttpClient httpClient() {
        return mock(HttpClient.class);
    }

    @Bean
    @Profile("offline")
    public SolrClient solrClient() throws Exception {
        System.setProperty(SOLR_DATA_DIR, file.getAbsolutePath());
        EmbeddedSolrServerFactory factory = new EmbeddedSolrServerFactory(solrHome);
        return factory.getSolrClient();
    }

    @Bean
    @Profile("offline")
    public SolrOperations solrOperations(SolrClient solrClient) {
        return new SolrTemplate(solrClient);
    }

    @Bean
    @Profile("job")
    public JobLauncherTestUtils utils() {
        return new JobLauncherTestUtils();
    }

    @Override
    public void destroy() throws Exception {
        if (file != null) {
            FileUtils.deleteDirectory(file);
            log.info("Deleted solr home");
        }
    }
}
