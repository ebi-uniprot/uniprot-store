package org.uniprot.store.indexer.proteome;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
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
import org.uniprot.store.indexer.genecentric.GeneCentricDocumentWriter;
import org.uniprot.store.job.common.converter.DocumentConverter;
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
    public ItemReader<Proteome> proteomeReader() throws IOException {
        return new ProteomeXmlEntryReader(proteomeXmlFilename);
    }

    @Bean(name = "proteomeXmlReader2")
    public StaxEventItemReader<Proteome> proteomeReader2() throws IOException {
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

    @Bean(name = "geneCentricItemWriter")
    public ItemWriter<Proteome> geneCentricItemWriter(UniProtSolrClient solrOperations) {
        return new GeneCentricDocumentWriter(solrOperations);
    }

    private DocumentConverter<Proteome, ProteomeDocument> proteomeEntryConverter() {
        return new ProteomeEntryConverter(createTaxonomyRepo());
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }

    @Bean(name = "proteomeGeneCentricItemWriter")
    public CompositeItemWriter<Proteome> proteomeCompositeWriter(UniProtSolrClient solrOperations) {
        CompositeItemWriter<Proteome> compositeWriter = new CompositeItemWriter<>();
        ItemWriter<Proteome> proteomeWriter = proteomeItemWriter(solrOperations);
        ItemWriter<Proteome> geneCentricWriter = geneCentricItemWriter(solrOperations);
        List<ItemWriter<? super Proteome>> writers = new ArrayList<>();
        writers.add(proteomeWriter);
        writers.add(geneCentricWriter);
        compositeWriter.setDelegates(writers);
        return compositeWriter;
    }
}
