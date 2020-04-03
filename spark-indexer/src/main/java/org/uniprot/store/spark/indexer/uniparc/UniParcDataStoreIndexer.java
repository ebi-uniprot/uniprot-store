package org.uniprot.store.spark.indexer.uniparc;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;

/**
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class UniParcDataStoreIndexer implements Runnable {

    private JavaSparkContext sparkContext;
    private ResourceBundle applicationConfig;
    private String releaseName;

    public UniParcDataStoreIndexer(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig, String releaseName) {
        this.sparkContext = sparkContext;
        this.applicationConfig = applicationConfig;
        this.releaseName = releaseName;
    }

    @Override
    public void run() {
        SparkConf sparkConf = sparkContext.sc().conf();

        JavaRDD<UniParcEntry> uniparcRDD =
                (JavaRDD<UniParcEntry>)
                        UniParcRDDTupleReader.load(sparkConf, applicationConfig, releaseName);

        String numberOfConnections =
                applicationConfig.getString("store.uniparc.numberOfConnections");
        String storeName = applicationConfig.getString("store.uniparc.storeName");
        String connectionURL = applicationConfig.getString("store.uniparc.host");

        uniparcRDD.foreachPartition(
                uniParcEntryIterator -> {
                    VoldemortClient<UniParcEntry> client =
                            new VoldemortRemoteUniParcEntryStore(
                                    Integer.valueOf(numberOfConnections), storeName, connectionURL);
                    int numNewConnection = 0;
                    while (uniParcEntryIterator.hasNext()) {
                        UniParcEntry entry = uniParcEntryIterator.next();
                        try {
                            client.saveEntry(entry);
                        } catch (Exception e) {
                            log.info("trying to reset voldemort connection...." + numNewConnection);
                            Thread.sleep(4000);
                            numNewConnection++;
                            if (numNewConnection < 3) {
                                client =
                                        new VoldemortRemoteUniParcEntryStore(
                                                Integer.valueOf(numberOfConnections),
                                                storeName,
                                                connectionURL);
                                client.saveEntry(entry);
                            } else {
                                throw new Exception(
                                        "Already tried to reset Voldemort connection twice and did not work",
                                        e);
                            }
                        }
                    }
                });
        log.info("Completed UniParc Data Store index");
    }
}
