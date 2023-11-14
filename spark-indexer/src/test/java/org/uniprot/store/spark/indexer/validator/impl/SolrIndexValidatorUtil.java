package org.uniprot.store.spark.indexer.validator.impl;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.spark.api.java.JavaSparkContext;
import org.mockito.Mockito;
import org.uniprot.store.spark.indexer.common.JobParameter;

import com.typesafe.config.Config;

public class SolrIndexValidatorUtil {

    static JobParameter getJobParameter(Config applicationConfig, JavaSparkContext context) {
        return JobParameter.builder()
                .releaseName("2020_02")
                .applicationConfig(applicationConfig)
                .sparkContext(context)
                .build();
    }

    static <T extends AbstractSolrIndexValidator> T spyMockValidator(
            T validatorImpl, long docCount, long solrCount) throws Exception {

        T validator = Mockito.spy(validatorImpl);

        Mockito.doReturn(docCount).when(validator).getOutputDocumentsCount();

        CloudSolrClient solrClient = Mockito.mock(CloudSolrClient.class);

        QueryResponse response = Mockito.mock(QueryResponse.class);
        SolrDocumentList queryResult = Mockito.mock(SolrDocumentList.class);
        Mockito.when(response.getResults()).thenReturn(queryResult);
        Mockito.when(queryResult.getNumFound()).thenReturn(solrCount);

        Mockito.when(solrClient.query(Mockito.eq(validator.getCollection().name()), Mockito.any()))
                .thenReturn(response);

        Mockito.doReturn(solrClient).when(validator).getSolrClient(Mockito.any());
        return validator;
    }
}
