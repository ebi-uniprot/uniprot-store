package org.uniprot.store.spark.indexer.main;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.validator.SolrIndexValidator;
import org.uniprot.store.spark.indexer.validator.SolrIndexValidatorFactory;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import static org.uniprot.store.spark.indexer.common.TaxDb.forName;

@Slf4j
public class SolrIndexValidatorMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 4) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name (for example: 2020_01)"
                            + "args[1]= collection name (for example: uniprot, uniparc or uniref)"
                            + "args[2]= spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)"
                            + "args[3]= taxonomy db (e.g.read or fly)");
        }

        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(applicationConfig, args[2])) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .taxDb(forName(args[3]))
                            .sparkContext(sparkContext)
                            .build();
            List<SolrCollection> solrCollections = SparkUtils.getSolrCollection(args[1]);
            SolrIndexValidatorFactory factory = new SolrIndexValidatorFactory();
            for (SolrCollection collection : solrCollections) {
                SolrIndexValidator validator =
                        factory.createSolrIndexValidator(collection, jobParameter);
                validator.runValidation();
            }
        } catch (Exception e) {
            throw new SparkIndexException("Unexpected error while validating solr index", e);
        } finally {
            log.info("Finished all jobs!");
        }
    }
}
