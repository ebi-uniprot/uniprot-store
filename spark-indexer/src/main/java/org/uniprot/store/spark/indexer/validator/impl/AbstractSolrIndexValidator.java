package org.uniprot.store.spark.indexer.validator.impl;

import static java.util.Collections.singletonList;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import java.util.*;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.validator.SolrIndexValidator;

/**
 * This class is used to validate Solr index. It queries solr and compare the result with count
 * retrieved from inputRDD and also saved documents in HPS.
 */
@Slf4j
public abstract class AbstractSolrIndexValidator implements SolrIndexValidator {

    private final JobParameter jobParameter;
    static final String SOLR_QUERY = "*:*";

    protected AbstractSolrIndexValidator(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public void runValidation() {
        try {
            long solrCount = getSolrCount();
            long rddCount = getRddCount(jobParameter);
            long outputCount = getOutputDocumentsCount();

            if (solrCount != rddCount || solrCount != outputCount) {
                throw new SparkIndexException(
                        "Total Entries does not match. "
                                + "Collection: "
                                + getCollection()
                                + ", DocumentOutput COUNT: "
                                + outputCount
                                + ", RDD COUNT: "
                                + rddCount
                                + ", Solr COUNT: "
                                + solrCount);
            } else {
                log.info(
                        "Collection: "
                                + getCollection()
                                + ", DocumentOutput COUNT: "
                                + outputCount
                                + ", RDD COUNT: "
                                + rddCount
                                + ", Solr COUNT: "
                                + solrCount);
            }
        } catch (SparkIndexException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexHPSDocumentsException(
                    "Unexpected error during " + getCollection() + " validation.", e);
        } finally {
            log.info("Finished check for " + getCollection());
        }
    }

    protected abstract long getRddCount(JobParameter jobParameter);

    protected abstract SolrCollection getCollection();

    protected abstract String getSolrFl();

    private long getSolrCount() {
        String zkHost = jobParameter.getApplicationConfig().getString("solr.zkhost");
        long solrCount = 0L;
        try (CloudSolrClient client = getSolrClient(zkHost)) {
            ModifiableSolrParams queryParams = new ModifiableSolrParams();
            queryParams.set("q", SOLR_QUERY);
            queryParams.set("fl", getSolrFl());
            QueryResponse result = client.query(getCollection().name(), queryParams);
            solrCount = result.getResults().getNumFound();
            log.info("totalEntries Solr COUNT: " + solrCount);
        } catch (Exception e) {
            log.error("Error executing " + getCollection().name() + " Validation: ", e);
        }
        return solrCount;
    }

    CloudSolrClient getSolrClient(String zkHost) {
        return new CloudSolrClient.Builder(singletonList(zkHost), Optional.empty()).build();
    }

    long getOutputDocumentsCount() {
        String hpsOutputFilePath =
                getCollectionOutputReleaseDirPath(
                        jobParameter.getApplicationConfig(),
                        jobParameter.getReleaseName(),
                        getCollection());
        return jobParameter
                .getSparkContext()
                .objectFile(hpsOutputFilePath)
                .map(obj -> (SolrInputDocument) obj)
                .count();
    }
}
