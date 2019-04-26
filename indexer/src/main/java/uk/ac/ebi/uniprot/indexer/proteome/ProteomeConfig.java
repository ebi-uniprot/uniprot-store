package uk.ac.ebi.uniprot.indexer.proteome;

import java.io.File;
import java.io.IOException;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
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
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.search.SolrCollection;
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
	public ItemWriter<ProteomeDocument> itemProteomeWriter(SolrTemplate solrTemplate) {
		return new SolrDocumentWriter<>(solrTemplate, SolrCollection.proteome);
	}

	private DocumentConverter<Proteome, ProteomeDocument> proteomeEntryConverter() {
		return new ProteomeEntryConverter(createTaxonomyRepo());
	}

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }
}
