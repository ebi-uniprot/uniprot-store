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
public class UniParcDataStoreIndexer {

    public static void indexDataStore(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig, String releaseName) {
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
                    while (uniParcEntryIterator.hasNext()) {
                        UniParcEntry entry = uniParcEntryIterator.next();
                        client.saveEntry(entry);
                    }
                });
        log.info("Completed UniParc Data Store index");
    }
}
