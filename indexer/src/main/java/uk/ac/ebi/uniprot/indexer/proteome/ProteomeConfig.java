package uk.ac.ebi.uniprot.indexer.proteome;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import lombok.Data;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.genecentric.GeneCentricDocumentWriter;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;

/**
 *
 * @author jluo
 * @date: 18 Apr 2019
 *
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
	public ItemWriter<Proteome> proteomeItemWriter(SolrTemplate solrTemplate) {
		return new ProteomeDocumentWriter(proteomeEntryProcessor(), solrTemplate);
	}
	
	@Bean(name = "geneCentricItemWriter")
	public ItemWriter<Proteome> geneCentricItemWriter(SolrTemplate solrTemplate) {
		return new GeneCentricDocumentWriter(solrTemplate);
	}

	private DocumentConverter<Proteome, ProteomeDocument> proteomeEntryConverter() {
		return new ProteomeEntryConverter(createTaxonomyRepo());
	}

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }
    
    @Bean(name = "proteomeGeneCentricItemWriter")
    public CompositeItemWriter<Proteome> proteomeCompositeWriter(SolrTemplate solrTemplate){
        CompositeItemWriter<Proteome > compositeWriter = new CompositeItemWriter<>();
        ItemWriter<Proteome>  proteomeWriter = proteomeItemWriter(solrTemplate);
        ItemWriter<Proteome>  geneCentricWriter = geneCentricItemWriter(solrTemplate);
        List<ItemWriter<? super Proteome>> writers = new ArrayList<>();
        writers.add(proteomeWriter);
        writers.add(geneCentricWriter);
        compositeWriter.setDelegates(writers);
        return compositeWriter;
    }
}
