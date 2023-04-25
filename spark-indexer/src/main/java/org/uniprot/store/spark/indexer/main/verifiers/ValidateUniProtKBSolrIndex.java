package org.uniprot.store.spark.indexer.main.verifiers;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexHDFSDocumentsException;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;

import static java.util.Collections.singletonList;

@Slf4j
public class ValidateUniProtKBSolrIndex {

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name (for example: 2020_01)");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        String zkHost = applicationConfig.getString("solr.zkhost");
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();
            long reviewed = 0;
            long reviewedIsoform = 0;
            long unReviewed = 0;
            try (CloudSolrClient client =
                         new CloudSolrClient.Builder(singletonList(zkHost), Optional.empty()).build()) {
                ModifiableSolrParams queryParams = new ModifiableSolrParams();
                queryParams.set("q", "reviewed:true AND active:true AND is_isoform:false");
                queryParams.set("fl", "accession_id");
                QueryResponse result = client.query(SolrCollection.uniprot.name(), queryParams);
                reviewed = result.getResults().getNumFound();
                log.info("reviewed Solr COUNT: "+reviewed);

                queryParams = new ModifiableSolrParams();
                queryParams.set("q", "reviewed:true AND active:true AND is_isoform:true");
                queryParams.set("fl", "accession_id");
                result = client.query(SolrCollection.uniprot.name(), queryParams);
                reviewedIsoform = result.getResults().getNumFound();
                log.info("reviewed isoform Solr COUNT: "+reviewedIsoform);

                queryParams = new ModifiableSolrParams();
                queryParams.set("q", "reviewed:false AND active:true");
                queryParams.set("fl", "accession_id");
                result = client.query(SolrCollection.uniprot.name(), queryParams);
                unReviewed = result.getResults().getNumFound();
                log.info("unReviewed Solr COUNT: "+unReviewed);
            } catch (Exception e) {
                log.error("Error executing uniprotkb Validation: ", e);
            }

            UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(jobParameter, false);
            long rddReviewed = reader.loadFlatFileToRDD()
                    .filter(entryStr -> {
                        String[] linesArray = entryStr.split("\n");
                        String idLine = linesArray[0];
                        String acLine = linesArray[1];
                        return idLine.contains("Reviewed;") && !acLine.split(";")[0].contains("-");
                    })
                    .filter(entryStr -> entryStr.contains("Reviewed;"))
                    .count();
            log.info("reviewed RDD COUNT: "+rddReviewed);

            long rddReviewedIsoform = reader.loadFlatFileToRDD()
                    .filter(entryStr -> {
                        String[] linesArray = entryStr.split("\n");
                        String idLine = linesArray[0];
                        String acLine = linesArray[1];
                        return idLine.contains("Reviewed;") && acLine.split(";")[0].contains("-");
                    })
                    .filter(entryStr -> entryStr.contains("Reviewed;"))
                    .count();
            log.info("reviewed isoform RDD COUNT: "+rddReviewedIsoform);

            long rddUnReviewed = reader.loadFlatFileToRDD()
                    .map(entryStr -> entryStr.split("\n")[0])
                    .filter(entryStr -> entryStr.contains("Unreviewed;"))
                    .count();
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
        } catch (Exception e) {
            throw new IndexHDFSDocumentsException("Unexpected error during index", e);
        } finally {
            log.info("Finished all Jobs!!!");
        }
    }
}
