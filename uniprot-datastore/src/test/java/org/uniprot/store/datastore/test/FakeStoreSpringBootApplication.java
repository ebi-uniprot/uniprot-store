/*
 * Created by sahmad on 28/01/19 19:14
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package org.uniprot.store.datastore.test;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
// @Import(StoreTestConfig.class)
public class FakeStoreSpringBootApplication {
    public static void main(String[] args) {
        SpringApplication.run(FakeStoreSpringBootApplication.class, args);
    }
}
