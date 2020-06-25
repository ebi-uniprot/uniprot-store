package org.uniprot.store.spark.indexer.uniprot;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidence;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidenceMapper;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidencesRDDReader;
import org.uniprot.store.spark.indexer.uniparc.UniParcRDDTupleReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniParcJoinMapper;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniParcMapper;

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
        UniProtKBRDDTupleReader uniprotkbReader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtKBEntry> uniprotRDD = uniprotkbReader.load();

        uniprotRDD = joinGoEvidences(uniprotRDD);
        uniprotRDD = joinUniParcId(uniprotRDD);

        saveInDataStore(uniprotRDD);
    }

    void saveInDataStore(JavaPairRDD<String, UniProtKBEntry> uniprotRDD) {
        ResourceBundle config = parameter.getApplicationConfig();
        String numberOfConnections = config.getString("store.uniprot.numberOfConnections");
        String storeName = config.getString("store.uniprot.storeName");
        String connectionURL = config.getString("store.uniprot.host");

        uniprotRDD
                .values()
                .foreachPartition(
                        uniProtEntryIterator -> {
                            VoldemortClient<UniProtKBEntry> client =
                                    new VoldemortRemoteUniProtKBEntryStore(
                                            Integer.parseInt(numberOfConnections),
                                            storeName,
                                            connectionURL);
                            DataStoreWriter<UniProtKBEntry> writer = new DataStoreWriter<>(client);
                            writer.indexInStore(uniProtEntryIterator);
                        });
        log.info("Completed UniProtKb Data Store index");
    }

    private JavaPairRDD<String, UniProtKBEntry> joinUniParcId(
            JavaPairRDD<String, UniProtKBEntry> uniprotRDD) {
        UniParcRDDTupleReader uniparcReader = new UniParcRDDTupleReader(parameter, false);
        // JavaPairRDD<accession, UniParcId>
        JavaPairRDD<String, String> uniparcJoinRdd =
                uniparcReader.load().flatMapToPair(new UniParcJoinMapper());
        uniprotRDD = uniprotRDD.leftOuterJoin(uniparcJoinRdd).mapValues(new UniParcMapper());
        return uniprotRDD;
    }

    private JavaPairRDD<String, UniProtKBEntry> joinGoEvidences(
            JavaPairRDD<String, UniProtKBEntry> uniprotRDD) {
        GOEvidencesRDDReader goEvidencesReader = new GOEvidencesRDDReader(parameter);
        JavaPairRDD<String, Iterable<GOEvidence>> goEvidenceRDD = goEvidencesReader.load();
        uniprotRDD = uniprotRDD.leftOuterJoin(goEvidenceRDD).mapValues(new GOEvidenceMapper());
        return uniprotRDD;
    }
}
