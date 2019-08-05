package org.uniprot.store.indexer.uniparc;

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
import org.uniprot.core.cv.taxonomy.FileNodeIterable;
import org.uniprot.core.cv.taxonomy.TaxonomyMapRepo;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.listener.LogRateListener;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.converter.DocumentConverter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

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
	public ItemWriter<UniParcDocument> geneCentricItemWriter(UniProtSolrOperations solrOperations) {
		return new SolrDocumentWriter<>(solrOperations, SolrCollection.uniparc);
	}

	private TaxonomyRepo createTaxonomyRepo() {
		return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
	}
}
