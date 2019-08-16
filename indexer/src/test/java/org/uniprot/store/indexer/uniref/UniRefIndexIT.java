package org.uniprot.store.indexer.uniref;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.uniprot.store.indexer.common.utils.Constants.UNIREF_INDEX_JOB;

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
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.listener.ListenerConfig;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniref.UniRefDocument;

/**
 *
 * @author jluo
 * @date: 15 Aug 2019
 *
 */

@ActiveProfiles(profiles = { "job", "offline" })
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { FakeIndexerSpringBootApplication.class, SolrTestConfig.class, UniRefIndexJob.class,
		UniRefIndexStep.class, ListenerConfig.class })
public class UniRefIndexIT {
	@Autowired
	private JobLauncherTestUtils jobLauncher;
	@Autowired
	private UniProtSolrOperations solrOperations;

	@Test
	void testIndexJob() throws Exception {
		JobExecution jobExecution = jobLauncher.launchJob();
		assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIREF_INDEX_JOB));

		BatchStatus status = jobExecution.getStatus();
		assertThat(status, is(BatchStatus.COMPLETED));

		Page<UniRefDocument> response = solrOperations.query(SolrCollection.uniref.name(), new SimpleQuery("*:*"),
				UniRefDocument.class);
		assertThat(response, is(notNullValue()));
		assertThat(response.getTotalElements(), is(2l));
		response.forEach(val -> verifyEntry(val));

	}

	private void verifyEntry(UniRefDocument doc) {
		String id = doc.getDocumentId();
		System.out.println(id);
		System.out.println(doc);
	
	}

}
