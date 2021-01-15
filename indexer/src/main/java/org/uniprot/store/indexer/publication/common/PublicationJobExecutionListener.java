package org.uniprot.store.indexer.publication.common;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBConfig;
import org.uniprot.store.search.SolrCollection;

/**
 * @author sahmad
 * @created 15/01/2021
 */
@Configuration
@Import({UniProtKBConfig.class})
public class PublicationJobExecutionListener implements JobExecutionListener {
    private final UniProtSolrClient uniProtSolrClient;

    public PublicationJobExecutionListener(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        // do nothing
    }

    // Hard commit contents of repository once job has finished.
    // Delegate all other commits to 'autoCommit' element of solrconfig.xml
    @Override
    public void afterJob(JobExecution jobExecution) {
        uniProtSolrClient.commit(SolrCollection.publication);
    }
}
