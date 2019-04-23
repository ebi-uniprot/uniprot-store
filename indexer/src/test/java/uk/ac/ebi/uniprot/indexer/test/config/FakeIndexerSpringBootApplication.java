/*
 * Created by sahmad on 28/01/19 19:14
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.indexer.test.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;


@EnableBatchProcessing
@SpringBootApplication
@Import(TestConfig.class)
public class FakeIndexerSpringBootApplication {
    public static void main(String[] args) {
        SpringApplication.run(FakeIndexerSpringBootApplication.class, args);
    }
}
