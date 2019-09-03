package org.uniprot.store.indexer.common.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.search.SolrCollection;

/**
 * @author lgonzales
 * @since 2019-09-03
 */
@Slf4j
public class SolrCommitStepListener implements StepExecutionListener {

    private UniProtSolrOperations solrOperations;

    public SolrCommitStepListener(UniProtSolrOperations solrOperations){
        this.solrOperations = solrOperations;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {

    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("I am about to commit in Solr");
        solrOperations.commit(SolrCollection.literature.name());
        log.info("Just committed in Solr");
        return stepExecution.getExitStatus();
    }
}
