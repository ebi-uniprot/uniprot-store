package org.uniprot.store.spark.indexer.main.verifiers;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexHDFSDocumentsException;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import java.io.IOException;
import java.util.Optional;
import java.util.ResourceBundle;

import static java.util.Collections.singletonList;
/**
 * This class is used to validate UniProtKB Index.
 * It counts and compare number of Swiss-Prot, Trembl and Isoforms (except canonical isoforms)
 * In order to compare, it queries solr and compare the result with count retrieved from uniprot-release.dat.
 */
@Slf4j
public class ValidateUniProtKBSolrIndexMain {

    static final String REVIEWED_QUERY = "reviewed:true AND active:true AND is_isoform:false";
    static final String ISOFORM_QUERY = "reviewed:true AND active:true AND is_isoform:true";
    static final String UNREVIEWED_QUERY = "reviewed:false AND active:true";

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name (for example: 2020_01)");
        }
        ValidateUniProtKBSolrIndexMain validator = new ValidateUniProtKBSolrIndexMain();
        validator.runValidation(args[0]);
    }

    boolean runValidation(String releaseName) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        String zkHost = applicationConfig.getString("solr.zkhost");
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(releaseName)
                            .sparkContext(sparkContext)
                            .build();
            long reviewed = 0;
            long reviewedIsoform = 0;
            long unReviewed = 0;
            try (CloudSolrClient client =
                         new CloudSolrClient.Builder(singletonList(zkHost), Optional.empty()).build()) {
                reviewed = getSolrCount(client, REVIEWED_QUERY);
                log.info("reviewed Solr COUNT: "+reviewed);

                reviewedIsoform = getSolrCount(client, ISOFORM_QUERY);
                log.info("reviewed isoform Solr COUNT: "+reviewedIsoform);

                unReviewed = getSolrCount(client, UNREVIEWED_QUERY);
                log.info("unReviewed Solr COUNT: "+unReviewed);
            } catch (Exception e) {
                log.error("Error executing uniprotkb Validation: ", e);
            }

            UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(jobParameter, false);
            JavaRDD<String> flatFileRDD = reader.loadFlatFileToRDD();

            long rddReviewed = getRDDReviewedCount(flatFileRDD);
            log.info("reviewed RDD COUNT: "+rddReviewed);

            long rddReviewedIsoform = getRDDReviewedIsoformCount(flatFileRDD);
            log.info("reviewed isoform RDD COUNT: "+rddReviewedIsoform);

            long rddUnReviewed = getRDDUnreviewedCount(flatFileRDD);
            log.info("unReviewed RDD COUNT: "+rddUnReviewed);

            if(reviewed != rddReviewed){
                throw new SparkIndexException("reviewed does not match. solr count: " + reviewed + " RDD count "+ rddReviewed);
            }
            if(reviewedIsoform != rddReviewedIsoform){
                throw new SparkIndexException("reviewed isoform does not match. solr count: " + reviewedIsoform + " RDD count "+ rddReviewedIsoform);
            }
            if(unReviewed != rddUnReviewed){
                throw new SparkIndexException("unReviewed does not match. solr count: " + unReviewed + " RDD count "+ rddUnReviewed);
            }
        } catch (SparkIndexException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexHDFSDocumentsException("Unexpected error during index", e);
        } finally {
            log.info("Finished all Jobs!!!");
        }
        return true;
    }

    long getRDDUnreviewedCount(JavaRDD<String> flatFileRDD) {
        return flatFileRDD
                .map(entryStr -> entryStr.split("\n")[0])
                .filter(entryStr -> entryStr.contains("Unreviewed;"))
                .count();
    }

    long getRDDReviewedIsoformCount(JavaRDD<String> flatFileRDD) {
        return flatFileRDD
                .filter(entryStr -> {
                    String[] linesArray = entryStr.split("\n");
                    String idLine = linesArray[0];
                    String acc = linesArray[1].split(";")[0];
                    return idLine.contains("Reviewed;") && acc.contains("-") && !acc.contains("-1");
                })
                .count();
    }

    long getRDDReviewedCount(JavaRDD<String> flatFileRDD) {
        return flatFileRDD
                .filter(entryStr -> {
                    String[] linesArray = entryStr.split("\n");
                    String idLine = linesArray[0];
                    String acLine = linesArray[1];
                    return idLine.contains("Reviewed;") && !acLine.split(";")[0].contains("-");
                })
                .count();
    }

    long getSolrCount(CloudSolrClient client, String query) throws SolrServerException, IOException {
        ModifiableSolrParams queryParams = new ModifiableSolrParams();
        queryParams.set("q", query);
        queryParams.set("fl", "accession_id");
        QueryResponse result = client.query(SolrCollection.uniprot.name(), queryParams);
        return result.getResults().getNumFound();
    }

}
