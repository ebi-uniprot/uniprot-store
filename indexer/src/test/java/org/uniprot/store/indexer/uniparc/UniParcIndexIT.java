package org.uniprot.store.indexer.uniparc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.json.parser.uniparc.UniParcJsonConfig;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.uniprot.store.indexer.common.utils.Constants.UNIPARC_INDEX_JOB;

/**
 *
 * @author jluo
 * @date: 18 Jun 2019
 *
*/
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, SolrTestConfig.class, UniParcIndexJob.class,
                           UniParcIndexStep.class, ListenerConfig.class})
public class UniParcIndexIT {
	 @Autowired
	    private JobLauncherTestUtils jobLauncher;
	    @Autowired
	    private UniProtSolrOperations solrOperations;

	    @Test
	    void testIndexJob() throws Exception {
	        JobExecution jobExecution = jobLauncher.launchJob();
	        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIPARC_INDEX_JOB));

	        BatchStatus status = jobExecution.getStatus();
	        assertThat(status, is(BatchStatus.COMPLETED));

	        Page<UniParcDocument> response = solrOperations
	                .query(SolrCollection.uniparc.name(), new SimpleQuery("*:*"), UniParcDocument.class);
	        assertThat(response, is(notNullValue()));
	        assertThat(response.getTotalElements(), is(3l));
	        response.forEach(val -> verifyEntry(val));
	       
	    }
	    private void verifyEntry(UniParcDocument doc) {
	    	String upi = doc.getDocumentId();
	    	ObjectMapper objectMapper = UniParcJsonConfig.getInstance().getFullObjectMapper();
	    	byte [] obj =doc.getEntryStored().array();
	    	try {
	    		UniParcEntry uniparc = objectMapper.readValue(obj, UniParcEntry.class);
	    	assertEquals(upi, uniparc.getUniParcId().getValue());
	    	}catch(Exception e) {
	    		fail(e.getMessage());
	    	}
	    }
	    	
}

