package org.uniprot.store.spark.indexer.main.verifiers;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.main.verifiers.mapper.UniProtKBPublicationMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 22/03/2021
 */
@Slf4j
public class VerifyCitationData {

    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        JobParameter jobParameter =
                JobParameter.builder()
                        .applicationConfig(applicationConfig)
                        .releaseName(args[0])
                        .sparkContext(sparkContext)
                        .build();

        JavaRDD<String> solrInputDocumentRDD =
                new UniProtKBRDDTupleReader(jobParameter,false).loadFlatFileToRDD();

        long withPubMed = solrInputDocumentRDD
                .flatMap(new UniProtKBPublicationMapper())
                .filter(VerifyCitationData::hasPubmedId)
                .count();
        log.debug("WithPubmed: {} ", withPubMed);

        long withoutPubMed = solrInputDocumentRDD
                .flatMap(new UniProtKBPublicationMapper())
                .filter(reference -> !hasPubmedId(reference))
                .count();
        log.debug("WithoutPubMed: {} ", withoutPubMed);
        sparkContext.close();
    }

    private static boolean hasPubmedId(UniProtKBReference reference) {
        return false;
    }
}
