package uk.ac.ebi.uniprot.indexer.uniprotkb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_FULL_INDEX_JOB;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

import java.util.List;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.proteome.ProteomeConfig;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.indexer.uniprotkb.proteome.UniProtKBProteomeIndexStep;
import uk.ac.ebi.uniprot.indexer.uniprotkb.step.UniProtKBStep;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

/**
 *
 * @author jluo
 * @date: 3 May 2019
 *
*/
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class, UniProtKBFullJob.class,
                           UniProtKBStep.class, UniProtKBProteomeIndexStep.class, ProteomeConfig.class, ListenerConfig.class})
public class UniProtKBFullJobIT {
	 @Autowired
	    private JobLauncherTestUtils jobLauncher;
	    @Autowired
	    private SolrTemplate template;

	    @Test
	    void testUniProtKBIndexingJob() throws Exception {
	        JobExecution jobExecution = jobLauncher.launchJob();
	        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIPROTKB_FULL_INDEX_JOB));

	        BatchStatus status = jobExecution.getStatus();
	        assertThat(status, is(BatchStatus.COMPLETED));

	        Page<UniProtDocument> response = template
	                .query(SolrCollection.uniprot.name(), new SimpleQuery("*:*"), UniProtDocument.class);
	        assertThat(response, is(notNullValue()));
	        assertThat(response.getTotalElements(), is(5L));
	        
//	        response = template
//	                .query(SolrCollection.uniprot.name(), new SimpleQuery("proteome_content:up000262260"), UniProtDocument.class);
//	        assertThat(response, is(notNullValue()));
//	        assertThat(response.getTotalElements(), is(4L));
	        
	        response = template
	                .query(SolrCollection.uniprot.name(), new SimpleQuery("genome_assembly:gca_003574175.1"), UniProtDocument.class);
	        assertThat(response, is(notNullValue()));
	        assertThat(response.getTotalElements(), is(4L));    
	        
	        response = template
	                .query(SolrCollection.uniprot.name(), new SimpleQuery("organism_id:35554"), UniProtDocument.class);
	        assertThat(response, is(notNullValue()));
	        assertThat(response.getTotalElements(), is(5L));    
	        List<UniProtDocument> results= response.getContent();
	        UniProtDocument result= results.get(0);
	        System.out.println("organism_id:35554");
		       results.stream().forEach(val -> System.out.println(val.accession));
	     
		        response = template
		                .query(SolrCollection.uniprot.name(), new SimpleQuery("accession:*"), UniProtDocument.class);
		        System.out.println("name:*");
		        results= response.getContent();
			       results.stream().forEach(val -> System.out.println(val.accession));
		        assertThat(response, is(notNullValue()));
		        assertThat(response.getTotalElements(), is(5L));    
		    
			       
			       
			       
	        response = template
	                .query(SolrCollection.uniprot.name(), new SimpleQuery("name:*"), UniProtDocument.class);
	       results= response.getContent();
	       results.stream().forEach(val -> System.out.println(val.accession));
	        assertThat(response, is(notNullValue()));
	        assertThat(response.getTotalElements(), is(5L));    
	    }

}

