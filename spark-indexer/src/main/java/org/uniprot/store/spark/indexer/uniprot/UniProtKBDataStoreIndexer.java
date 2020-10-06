package org.uniprot.store.spark.indexer.uniprot;

import java.util.Iterator;
import java.util.ResourceBundle;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidence;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidenceMapper;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidencesRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtKBAnnotationScoreMapper;
import org.uniprot.store.spark.indexer.uniprot.writer.UniProtKBDataStoreWriter;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniProtKBDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public UniProtKBDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        try { // Try system lib path first
            System.loadLibrary("brotli");
        } catch (UnsatisfiedLinkError linkError) {
            log.error("UnsatisfiedLinkError from indexInDataStore:", linkError);
        }

        String mappedLibrary = System.mapLibraryName("brotli");
        log.info("System.mapLibraryName: {}",mappedLibrary);

        System.getenv().forEach((key, value) -> log.info("ENV_VAR_KEY: {} ENV_VAR_VALUE: {}", key ,value));

        ResourceBundle config = parameter.getApplicationConfig();
        GOEvidencesRDDReader goEvidencesReader = new GOEvidencesRDDReader(parameter);
        UniProtKBRDDTupleReader uniprotkbReader = new UniProtKBRDDTupleReader(parameter, false);

        JavaPairRDD<String, UniProtKBEntry> uniprotRDD = uniprotkbReader.load();
        JavaPairRDD<String, Iterable<GOEvidence>> goEvidenceRDD = goEvidencesReader.load();

        String numberOfConnections = config.getString("store.uniprot.numberOfConnections");
        String storeName = config.getString("store.uniprot.storeName");
        String connectionURL = config.getString("store.uniprot.host");

        log.info("Checking brotli. isBrotliAvailable: {}", BrotliLoader.isBrotliAvailable());
        uniprotRDD
                .mapValues(new UniProtKBAnnotationScoreMapper())
                .leftOuterJoin(goEvidenceRDD)
                .mapValues(new GOEvidenceMapper())
                .values()
                .foreachPartition(getWriter(numberOfConnections, storeName, connectionURL));

        log.info("Completed UniProtKb Data Store index. and brotli {}",BrotliLoader.isBrotliAvailable());
    }

    VoidFunction<Iterator<UniProtKBEntry>> getWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        return new UniProtKBDataStoreWriter(numberOfConnections, storeName, connectionURL);
    }
}
