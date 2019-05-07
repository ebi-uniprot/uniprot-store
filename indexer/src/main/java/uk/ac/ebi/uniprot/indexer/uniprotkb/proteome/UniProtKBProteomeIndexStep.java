package uk.ac.ebi.uniprot.indexer.uniprotkb.proteome;

import java.io.File;
import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import uk.ac.ebi.uniprot.indexer.common.listener.LogRateListener;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;

/**
 *
 * @author jluo
 * @date: 2 May 2019
 *
*/
@Configuration
public class UniProtKBProteomeIndexStep {
	
	
	 private final StepBuilderFactory stepBuilderFactory;
	  @Value(("${solr.indexing.chunkSize}"))
	  private int chunkSize=100;
	 
	 @Value(("${proteome.indexing.xml.file}"))
		private String proteomeXmlFilename;
	 
		@Value(("${uniprotkb.indexing.taxonomyFile}"))
		private String taxonomyFile;
	    @Autowired
	    public UniProtKBProteomeIndexStep(StepBuilderFactory stepBuilderFactory) {
	        this.stepBuilderFactory = stepBuilderFactory;

	    }
	    	    
		@Bean(name = "uniProtKBProteomeItemWriter")
		public ItemWriter<Proteome> itemProteomeWriter(SolrClient solrClient) {
			return new UniProtKBProteomeWriter(solrClient, SolrCollection.uniprot, createTaxonomyRepo());
		}

	    @Bean("UniProtKBProteomeIndexStep")
	    public Step uniProtKBProteomeIndexViaXmlStep(
	    		 StepExecutionListener stepListener,
                ChunkListener chunkListener,
	    		 @Qualifier("proteomeXmlReader2")  ItemReader<Proteome> itemReader,
	    		 @Qualifier("uniProtKBProteomeItemWriter") ItemWriter<Proteome> itemWriter) {
	        return this.stepBuilderFactory.get("UniProtKB_Proteome_Index_Step")

	                .<Proteome, Proteome>chunk(chunkSize)
	                .reader(itemReader)
	                .writer(itemWriter)
	                .listener(stepListener)
	                .listener(chunkListener)
	                .listener(new LogRateListener<ProteomeDocument>())
	                .build();
	    }
	    
	    private TaxonomyRepo createTaxonomyRepo() {
	        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
	    }
}

