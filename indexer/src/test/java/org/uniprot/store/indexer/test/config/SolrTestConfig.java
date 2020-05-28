/*
 * Created by sahmad on 07/02/19 16:44
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package org.uniprot.store.indexer.test.config;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;

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
    public SolrClient apacheSolrClient() throws Exception {
        System.setProperty(SOLR_DATA_DIR, file.getAbsolutePath());
        return new EmbeddedSolrServer(createCoreContainer(solrHome), "collection1");
    }

    private CoreContainer createCoreContainer(String solrHomeDirectory)
            throws UnsupportedEncodingException {
        solrHomeDirectory = URLDecoder.decode(solrHomeDirectory, "utf-8");
        return CoreContainer.createAndLoad(FileSystems.getDefault().getPath(solrHomeDirectory));
    }

    @Bean(destroyMethod = "cleanUp")
    @Profile("offline")
    public UniProtSolrClient solrClient(SolrClient apacheSolrClient) {
        return new UniProtSolrClient(apacheSolrClient);
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
