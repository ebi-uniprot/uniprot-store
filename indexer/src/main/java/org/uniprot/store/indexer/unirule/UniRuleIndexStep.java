package org.uniprot.store.indexer.unirule;

import static org.uniprot.store.indexer.common.utils.Constants.UNIRULE_INDEX_STEP;

import java.io.File;

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
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

/**
 * @author sahmad
 * @date: 12 May 2020
 */
@Configuration
public class UniRuleIndexStep {
    @Value(("${unirule.indexing.chunkSize}"))
    private int chunkSize = 50;

    @Value(("${unirule.indexing.xml.file}"))
    private String filePath;

    @Value(("${uniprotkb.indexing.taxonomyFile}"))
    private String taxonomyFile;

    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public UniRuleIndexStep(StepBuilderFactory stepBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Step indexUniRuleStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<UniRuleType> uniRuleReader,
            ItemProcessor<UniRuleType, UniRuleDocument> uniRuleProcessor,
            ItemWriter<UniRuleDocument> uniRuleWriter) {
        return this.stepBuilderFactory
                .get(UNIRULE_INDEX_STEP)
                .<UniRuleType, UniRuleDocument>chunk(chunkSize)
                .reader(uniRuleReader)
                .processor(uniRuleProcessor)
                .writer(uniRuleWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new LogRateListener<UniRuleDocument>())
                .build();
    }

    @Bean
    public ItemReader<UniRuleType> uniRuleReader() {
        return new UniRuleXmlEntryReader(filePath);
    }

    @Bean
    public ItemProcessor<UniRuleType, UniRuleDocument> uniRuleProcessor() {
        return new UniRuleProcessor(createTaxonomyRepo());
    }

    @Bean
    public ItemWriter<UniRuleDocument> uniRuleWriter(UniProtSolrClient solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.unirule);
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }
}
