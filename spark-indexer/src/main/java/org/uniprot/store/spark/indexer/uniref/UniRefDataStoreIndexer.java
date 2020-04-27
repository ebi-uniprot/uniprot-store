package org.uniprot.store.spark.indexer.uniref;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniRefDataStoreIndexer implements Runnable {

    private final JobParameter jobParameter;

    public UniRefDataStoreIndexer(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public void run() {
        indexUniRef(UniRefType.UniRef50, jobParameter);
        indexUniRef(UniRefType.UniRef90, jobParameter);
        indexUniRef(UniRefType.UniRef100, jobParameter);
    }

    private void indexUniRef(UniRefType type, JobParameter jobParameter) {
        SparkConf sparkConf = jobParameter.getSparkContext().sc().conf();
        ResourceBundle config = jobParameter.getApplicationConfig();
        String releaseName = jobParameter.getReleaseName();

        String numberOfConnections = config.getString("store.uniref.numberOfConnections");
        String storeName = config.getString("store.uniref.storeName");
        String connectionURL = config.getString("store.uniref.host");

        JavaRDD<UniRefEntry> uniRefRDD =
                UniRefRDDTupleReader.load(type, sparkConf, config, releaseName);
        uniRefRDD.foreachPartition(
                uniProtEntryIterator -> {
                    VoldemortClient<UniRefEntry> client =
                            new VoldemortRemoteUniRefEntryStore(
                                    Integer.parseInt(numberOfConnections),
                                    storeName,
                                    connectionURL);
                    DataStoreWriter<UniRefEntry> writer = new DataStoreWriter<>(client);
                    writer.indexInStore(uniProtEntryIterator);
                });
        log.info("Completed UniRef Data Store index");
    }
}
