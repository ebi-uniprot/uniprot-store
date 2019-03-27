/*
 * Created by sahmad on 28/01/19 19:14
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class IndexerSpringBootApplication {
    @Value(("${indexer.xref.solr.url}"))
    private String solrUrl;

    public static void main(String[] args) {
        SpringApplication.run(IndexerSpringBootApplication.class, args);
    }


    @Bean
    public SolrClient solrClient(){
        return new HttpSolrClient.Builder(this.solrUrl).build();
    }

}
