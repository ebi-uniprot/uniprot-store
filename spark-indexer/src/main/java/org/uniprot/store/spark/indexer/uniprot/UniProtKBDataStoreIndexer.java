package org.uniprot.store.spark.indexer.uniprot;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniProtKBDataStoreIndexer implements Runnable {

    private JavaSparkContext sparkContext;
    private ResourceBundle applicationConfig;
    private String releaseName;

    public UniProtKBDataStoreIndexer(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig, String releaseName) {
        this.sparkContext = sparkContext;
        this.applicationConfig = applicationConfig;
        this.releaseName = releaseName;
    }

    @Override
    public void run() {
        JavaPairRDD<String, UniProtEntry> uniprotRDD =
                (JavaPairRDD<String, UniProtEntry>)
                        UniProtKBRDDTupleReader.load(sparkContext, applicationConfig, releaseName);

        String numberOfConnections =
                applicationConfig.getString("store.uniprot.numberOfConnections");
        String storeName = applicationConfig.getString("store.uniprot.storeName");
        String connectionURL = applicationConfig.getString("store.uniprot.host");

        uniprotRDD.foreachPartition(
                uniProtEntryIterator -> {
                    VoldemortClient<UniProtEntry> client =
                            new VoldemortRemoteUniProtKBEntryStore(
                                    Integer.valueOf(numberOfConnections), storeName, connectionURL);
                    int numNewConnection = 0;
                    while (uniProtEntryIterator.hasNext()) {
                        Tuple2<String, UniProtEntry> tuple2 = uniProtEntryIterator.next();
                        try {
                            client.saveEntry(tuple2._2);
                        } catch (Exception e) {
                            log.info("trying to reset voldemort connection...." + numNewConnection);
                            Thread.sleep(4000);
                            numNewConnection++;
                            if (numNewConnection < 3) {
                                client =
                                        new VoldemortRemoteUniProtKBEntryStore(
                                                Integer.valueOf(numberOfConnections),
                                                storeName,
                                                connectionURL);
                                client.saveEntry(tuple2._2);
                            } else {
                                throw new Exception(
                                        "Already tried to reset Voldemort connection twice and did not work",
                                        e);
                            }
                        }
                    }
                });
        log.info("Completed UniProtKb Data Store index");
    }
}
