package uk.ac.ebi.uniprot.indexer.uniparc;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrOperations;
import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.indexer.common.listener.LogRateListener;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.search.document.uniparc.UniParcDocument;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.Entry;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author jluo
 * @date: 18 Jun 2019
 *
 */

@Configuration
public class UniParcIndexStep {
	
	@Value(("${solr.indexing.chunkSize}"))
	private int chunkSize = 100;
	@Value(("${uniparc.indexing.xml.file}"))
	private String uniparcXmlFilename;

	@Value(("${uniprotkb.indexing.taxonomyFile}"))
	private String taxonomyFile;

	private final StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public UniParcIndexStep(StepBuilderFactory stepBuilderFactory) {
		this.stepBuilderFactory = stepBuilderFactory;
	}

	@Bean("UniParcIndexStep")
	public Step proteomeIndexViaXmlStep(StepExecutionListener stepListener, ChunkListener chunkListener,
			ItemReader<Entry> itemReader, ItemProcessor<Entry, UniParcDocument> itemProcessor,
			ItemWriter<UniParcDocument> itemWriter) {
		return this.stepBuilderFactory.get("UniParc_Index_Step").<Entry, UniParcDocument>chunk(chunkSize)
				.reader(itemReader).processor(itemProcessor).writer(itemWriter).listener(stepListener)
				.listener(chunkListener).listener(new LogRateListener<ProteomeDocument>()).build();
	}

	@Bean
	public ItemReader<Entry> uniparcReader() throws IOException {
		return new UniParcXmlEntryReader(uniparcXmlFilename);
	}

	@Bean
	public ItemProcessor<Entry, UniParcDocument> uniparcEntryProcessor() {
		return new UniParcEntryProcessor(uniparcEntryConverter());
	}

    private DocumentConverter<Entry, UniParcDocument> uniparcEntryConverter() {
        return new UniParcDocumentConverter(createTaxonomyRepo());
    }
	
	
	@Bean
	public ItemWriter<UniParcDocument> geneCentricItemWriter(SolrOperations solrOperations) {
		return new SolrDocumentWriter<>(solrOperations, SolrCollection.uniparc);
	}

	private TaxonomyRepo createTaxonomyRepo() {
		return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
	}
}
