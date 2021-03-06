package org.uniprot.store.indexer.uniparc;

import static org.uniprot.store.indexer.uniprotkb.config.SuggestionConfig.DEFAULT_TAXON_SYNONYMS_FILE;
import static org.uniprot.store.indexer.uniprotkb.config.SuggestionConfig.loadDefaultTaxonSynonymSuggestions;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * @author jluo
 * @date: 18 Jun 2019
 */
@Configuration
public class UniParcIndexStep {

    @Value(("${uniparc.indexing.chunkSize}"))
    private int chunkSize = 1000;

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
    public Step uniparcIndexViaXmlStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<Entry> itemReader,
            ItemProcessor<Entry, UniParcDocument> itemProcessor,
            ItemWriter<UniParcDocument> itemWriter) {
        return this.stepBuilderFactory
                .get("UniParc_Index_Step")
                .<Entry, UniParcDocument>chunk(chunkSize)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new LogRateListener<UniParcDocument>())
                .build();
    }

    @Bean
    public ItemReader<Entry> uniparcReader() {
        return new UniParcXmlEntryReader(uniparcXmlFilename);
    }

    @Bean
    public ItemProcessor<Entry, UniParcDocument> uniparcEntryProcessor() {
        return new UniParcEntryProcessor(uniparcEntryConverter());
    }

    private UniParcDocumentConverter uniparcEntryConverter() {
        return new UniParcDocumentConverter(createTaxonomyRepo(), commonTaxonomySuggestions());
    }

    private Map<String, SuggestDocument> commonTaxonomySuggestions() {
        Map<String, SuggestDocument> suggestionMap = new ConcurrentHashMap<>();

        List<SuggestDocument> defaultDocs =
                loadDefaultTaxonSynonymSuggestions(
                        SuggestDictionary.UNIPARC_TAXONOMY, DEFAULT_TAXON_SYNONYMS_FILE);

        defaultDocs.forEach(
                suggestion ->
                        suggestionMap.put(
                                SuggestDictionary.UNIPARC_TAXONOMY.name() + ":" + suggestion.id,
                                suggestion));
        return suggestionMap;
    }

    @Bean
    public ItemWriter<UniParcDocument> uniparcItemWriter(UniProtSolrClient solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.uniparc);
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }
}
