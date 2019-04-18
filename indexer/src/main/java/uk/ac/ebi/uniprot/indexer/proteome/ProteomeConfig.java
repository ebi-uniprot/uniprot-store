package uk.ac.ebi.uniprot.indexer.proteome;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

/**
 *
 * @author jluo
 * @date: 18 Apr 2019
 *
*/
@Configuration
@Data
public class ProteomeConfig {
	  @Value(("${proteome.xml.file}"))
	  private String filename;
	  
	  @Bean("ProteomeEntryProcessor")
	  public ProteomeEntryProcessor proteomeEntryProcessor() {
		  return 
	  }
}

