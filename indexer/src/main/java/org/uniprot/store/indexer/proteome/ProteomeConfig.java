package org.uniprot.store.indexer.proteome;

import java.io.File;

import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.job.common.util.CommonConstants;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

import lombok.Data;

/**
 * @author jluo
 * @date: 18 Apr 2019
 */
@Configuration
@Data
public class ProteomeConfig {
    @Value(("${proteome.indexing.xml.file}"))
    private String proteomeXmlFilename;

    @Value(("${uniprotkb.indexing.taxonomyFile}"))
    private String taxonomyFile;

    @Value(("${proteome.genecentric.canonical.dir.path}"))
    private String geneCentricDir;

    @Value(("${proteome.genecentric.canonical.file.suffix}"))
    private String geneCentricFileSuffix;

    @Bean(name = "proteomeXmlReader")
    public ItemReader<Proteome> proteomeReader() {
        return new ProteomeXmlEntryReader(proteomeXmlFilename);
    }

    @Bean("ProteomeDocumentProcessor")
    public ItemProcessor<Proteome, ProteomeDocument> proteomeEntryProcessor(
            ProteomeDocumentConverter proteomeEntryConverter,
            ProteomeEntryAdapter proteomeEntryAdapter) {
        return new ProteomeItemProcessor(proteomeEntryConverter, proteomeEntryAdapter);
    }

    @Bean(name = "proteomeItemWriter")
    public ItemWriter<ProteomeDocument> proteomeItemWriter(UniProtSolrClient solrOperations) {
        return new ProteomeDocumentWriter(solrOperations);
    }

    @Bean(name = "proteomeEntryConverter")
    public ProteomeDocumentConverter proteomeEntryConverter(TaxonomyRepo taxonomyRepo) {
        return new ProteomeDocumentConverter(taxonomyRepo);
    }

    @Bean("ProteomeEntryAdapter")
    public ProteomeEntryAdapter proteomeEntryAdapter(TaxonomyRepo taxonomyRepo) {
        return new ProteomeEntryAdapter(taxonomyRepo, geneCentricDir, geneCentricFileSuffix);
    }

    @Bean("TaxonomyRepo")
    public TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener =
                new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(
                new String[] {
                    CommonConstants.FAILED_ENTRIES_COUNT_KEY,
                    CommonConstants.WRITTEN_ENTRIES_COUNT_KEY,
                    Constants.SUGGESTIONS_MAP
                });
        return executionContextPromotionListener;
    }
}
