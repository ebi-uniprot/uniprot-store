package org.uniprot.store.spark.indexer.uniref;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniRefDataStoreIndexer implements Runnable {

    private JavaSparkContext sparkContext;
    private ResourceBundle applicationConfig;
    private String releaseName;

    public UniRefDataStoreIndexer(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig, String releaseName) {
        this.sparkContext = sparkContext;
        this.applicationConfig = applicationConfig;
        this.releaseName = releaseName;
    }

    @Override
    public void run() {
        SparkConf sparkConf = sparkContext.sc().conf();
        String numberOfConnections =
                applicationConfig.getString("store.uniprot.numberOfConnections");
        String storeName = applicationConfig.getString("store.uniprot.storeName");
        String connectionURL = applicationConfig.getString("store.uniprot.host");

        JavaRDD<UniRefEntry> uniRefRDD =
                (JavaRDD<UniRefEntry>)
                        UniRefRDDTupleReader.load(
                                        UniRefType.UniRef50,
                                        sparkConf,
                                        applicationConfig,
                                        releaseName)
                                .union(
                                        UniRefRDDTupleReader.load(
                                                UniRefType.UniRef90,
                                                sparkConf,
                                                applicationConfig,
                                                releaseName))
                                .union(
                                        UniRefRDDTupleReader.load(
                                                UniRefType.UniRef100,
                                                sparkConf,
                                                applicationConfig,
                                                releaseName));

        uniRefRDD.foreachPartition(
                uniProtEntryIterator -> {
                    VoldemortClient<UniRefEntry> client =
                            new VoldemortRemoteUniRefEntryStore(
                                    Integer.valueOf(numberOfConnections), storeName, connectionURL);
                    int numNewConnection = 0;
                    while (uniProtEntryIterator.hasNext()) {
                        UniRefEntry entry = uniProtEntryIterator.next();
                        try {
                            client.saveEntry(entry);
                        } catch (Exception e) {
                            log.info("trying to reset voldemort connection...." + numNewConnection);
                            Thread.sleep(4000);
                            numNewConnection++;
                            if (numNewConnection < 3) {
                                client =
                                        new VoldemortRemoteUniRefEntryStore(
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
        log.info("Completed UniRef Data Store index");
    }
}
