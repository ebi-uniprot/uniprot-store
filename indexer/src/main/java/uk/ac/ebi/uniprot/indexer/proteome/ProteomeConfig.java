package uk.ac.ebi.uniprot.indexer.proteome;

import java.io.File;
import java.io.IOException;

import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.Data;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
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
	  
	  @Bean("ProteomeEntryProcessor")
	  public ProteomeEntryProcessor proteomeEntryProcessor() {
		  return new ProteomeEntryProcessor(createTaxonomyRepo());
	  }
	  
	    @Bean(name = "proteomeXmlReader")
	    public ItemReader<Proteome> xrefReader() throws IOException {
	        return new ProteomeXmlEntryReader(proteomeXmlFilename);
	    }
	  
	    private TaxonomyRepo createTaxonomyRepo() {
	        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
	    }
}

