package org.uniprot.store.spark.indexer.main.experimental;

import java.io.Serial;
import java.util.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortRemoteUniParcCrossReferenceStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniparc.UniParcLightRDDTupleReader;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UniParcCrossReferenceValidator {

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name"
                            + "args[1]=spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)");
        }
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(applicationConfig, args[1])) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();

            UniParcLightRDDTupleReader reader = new UniParcLightRDDTupleReader(parameter, false);
            JavaRDD<UniParcEntryLight> uniParcLightRDD = reader.load();

            DataStoreParameter dataStoreParameter = getDataStoreParameter(applicationConfig);
            int batchSize = applicationConfig.getInt("store.uniparc.cross.reference.batchSize");
            uniParcLightRDD.foreachPartition(
                    new CheckVoldermortXref(dataStoreParameter, batchSize));
        } catch (Exception e) {
            throw new IndexDataStoreException("Unexpected error during DataStore validation", e);
        } finally {
            log.info("All jobs finished!!!");
        }
    }

    private static DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections =
                config.getString("store.uniparc.cross.reference.numberOfConnections");
        String maxRetry = config.getString("store.uniparc.cross.reference.retry");
        String delay = config.getString("store.uniparc.cross.reference.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniparc.cross.reference.host"))
                .storeName(config.getString("store.uniparc.cross.reference.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(true)
                .brotliLevel(11)
                .build();
    }

    static class CheckVoldermortXref implements VoidFunction<Iterator<UniParcEntryLight>> {

        @Serial private static final long serialVersionUID = -4603525615443900815L;
        private final DataStoreParameter parameter;

        private final int batchSize;

        public CheckVoldermortXref(DataStoreParameter parameter, int batchSize) {
            this.parameter = parameter;
            this.batchSize = batchSize;
        }

        @Override
        public void call(Iterator<UniParcEntryLight> entryIterator) {
            List<String> issueMessages = new ArrayList<>();
            try (VoldemortClient<UniParcCrossReferencePair> client = getDataStoreClient()) {
                while (entryIterator.hasNext()) {
                    final UniParcEntryLight entry = entryIterator.next();
                    if (entry.getCrossReferenceCount() > 0) {
                        int totalPage = entry.getCrossReferenceCount() / batchSize;
                        int xrefTotalCount = 0;
                        for (int i = 0; i <= totalPage; i++) {
                            String pageKey = entry.getUniParcId() + "_" + i;
                            Optional<UniParcCrossReferencePair> xrefPair = client.getEntry(pageKey);
                            if (xrefPair.isPresent()) {
                                xrefTotalCount += xrefPair.get().getValue().size();
                            } else {
                                issueMessages.add("Unable to find Page: " + pageKey);
                            }
                        }
                        if (xrefTotalCount != entry.getCrossReferenceCount()) {
                            issueMessages.add(
                                    "Entry count mismatch for UniParcID: "
                                            + entry.getUniParcId()
                                            + " entry count: "
                                            + entry.getCrossReferenceCount()
                                            + " with total found in store "
                                            + xrefTotalCount
                                            + " ");
                        }
                    } else {
                        issueMessages.add(
                                "UniParcID: "
                                        + entry.getUniParcId()
                                        + " does not have Cross Reference");
                    }
                }
            }
            if (!issueMessages.isEmpty()) {
                throw new IndexDataStoreException(
                        "ISSUES FOUND: " + String.join(" AND ", issueMessages));
            }
        }

        protected VoldemortClient<UniParcCrossReferencePair> getDataStoreClient() {
            return new VoldemortRemoteUniParcCrossReferenceStore(
                    parameter.getNumberOfConnections(),
                    parameter.isBrotliEnabled(),
                    parameter.getBrotliLevel(),
                    parameter.getStoreName(),
                    parameter.getConnectionURL());
        }
    }
}
