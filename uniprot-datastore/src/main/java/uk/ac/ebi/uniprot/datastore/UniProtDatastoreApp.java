package uk.ac.ebi.uniprot.datastore;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * @author jluo
 * @date: 18 Apr 2019
 *
 */
@SpringBootApplication
@EnableBatchProcessing
public class UniProtDatastoreApp {
	public static void main(String[] args) {
		SpringApplication.run(UniProtDatastoreApp.class, args);
	}
}
