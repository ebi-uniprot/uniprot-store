package org.uniprot.store.indexer.proteome;

import java.io.File;

import lombok.Data;

import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.job.common.util.CommonConstants;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

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

    @Bean(name = "proteomeXmlReader")
    public ItemReader<Proteome> proteomeReader() {
        return new ProteomeXmlEntryReader(proteomeXmlFilename);
    }

    @Bean(name = "proteomeXmlReader2")
    public StaxEventItemReader<Proteome> proteomeReader2() {
        return new StaxEventItemReaderBuilder<Proteome>()
                .name("proteomeXmlReader2")
                .resource(new FileSystemResource(proteomeXmlFilename))
                .addFragmentRootElements("proteome")
                .unmarshaller(proteomeMarshaller())
                .build();
    }

    @Bean
    public Unmarshaller proteomeMarshaller() {
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(Proteome.class);
        return marshaller;
    }

    @Bean("ProteomeDocumentProcessor")
    public ItemProcessor<Proteome, ProteomeDocument> proteomeEntryProcessor() {
        return new ProteomeDocumentProcessor(proteomeEntryConverter());
    }

    @Bean(name = "proteomeItemWriter")
    public ItemWriter<Proteome> proteomeItemWriter(UniProtSolrClient solrOperations) {
        return new ProteomeDocumentWriter(proteomeEntryProcessor(), solrOperations);
    }

    private DocumentConverter<Proteome, ProteomeDocument> proteomeEntryConverter() {
        return new ProteomeEntryConverter(createTaxonomyRepo());
    }

    private TaxonomyRepo createTaxonomyRepo() {
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
