package org.uniprot.store.spark.indexer.main.verifiers;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.UniParcId;
import org.uniprot.core.uniprotkb.UniProtKBAccession;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniref.UniRefEntryId;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortRemoteUniRefEntryLightStore;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniparc.UniParcRDDTupleReader;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import org.uniprot.store.spark.indexer.uniref.UniRefLightRDDTupleReader;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

@Slf4j
public class StressTestVoldemort {

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected args[0]= release name");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();
            JavaFutureAction<Void> uniProt = loadUniProt(jobParameter);
            JavaFutureAction<Void> uniParc = loadUniParc(jobParameter);
            JavaFutureAction<Void> uniRef = loadUniRef(jobParameter);
            uniProt.get();
            uniParc.get();
            uniRef.get();
        } catch (Exception e) {
            throw new IndexDataStoreException("Unexpected error during DataStore index", e);
        } finally {
            log.info("All jobs finished!!!");
        }
    }

    private static JavaFutureAction<Void> loadUniRef(JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        DataStoreParameter parameter = getDataStoreParameter(config, "uniref.light");

        UniRefLightRDDTupleReader reader = new UniRefLightRDDTupleReader(UniRefType.UniRef100, jobParameter, false);
        return reader.load()
                .map(UniRefEntryLight::getId)
                .foreachPartitionAsync(new IterateUniRef(parameter));
    }

    private static JavaFutureAction<Void> loadUniParc(JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        DataStoreParameter parameter = getDataStoreParameter(config, "uniparc");

        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(jobParameter, false);
        return reader.load()
                .map(UniParcEntry::getUniParcId)
                .foreachPartitionAsync(new IterateUniParc(parameter));
    }

    private static JavaFutureAction<Void> loadUniProt(JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        DataStoreParameter parameter = getDataStoreParameter(config, "uniprot");

        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(jobParameter, false);
        return reader.loadFlatFileToRDD()
                .map(new FlatFileToPrimaryAccession())
                .foreachPartitionAsync(new IterateUniProt(parameter));
    }

    private static DataStoreParameter getDataStoreParameter(ResourceBundle config, String storeProperty) {
        String numberOfConnections = config.getString("store."+storeProperty+".numberOfConnections");
        String maxRetry = config.getString("store."+storeProperty+".retry");
        String delay = config.getString("store."+storeProperty+".delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store."+storeProperty+".host"))
                .storeName(config.getString("store."+storeProperty+".storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .build();
    }

    private static class IterateUniRef implements VoidFunction<Iterator<UniRefEntryId>> {

        private final DataStoreParameter parameter;

        public IterateUniRef(DataStoreParameter parameter){
            this.parameter = parameter;
        }

        @Override
        public void call(Iterator<UniRefEntryId> uniRefEntryIdIterator) throws Exception {
            VoldemortClient<UniRefEntryLight> client = new VoldemortRemoteUniRefEntryLightStore(
                    parameter.getNumberOfConnections(),
                    parameter.getStoreName(),
                    parameter.getConnectionURL());

            while (uniRefEntryIdIterator.hasNext()) {
                final UniRefEntryId entry = uniRefEntryIdIterator.next();
                Optional<UniRefEntryLight> result = client.getEntry(entry.getValue());
                if(result.isEmpty()){
                    throw new IndexDataStoreException("Unable to find entry: "+entry.getValue());
                }
            }
        }

    }

    private static class IterateUniProt implements VoidFunction<Iterator<UniProtKBAccession>> {

        private final DataStoreParameter parameter;

        public IterateUniProt(DataStoreParameter parameter){
            this.parameter = parameter;
        }

        @Override
        public void call(Iterator<UniProtKBAccession> uniProtKBAccessionIterator) throws Exception {
            VoldemortClient<UniProtKBEntry> client = new VoldemortRemoteUniProtKBEntryStore(
                    parameter.getNumberOfConnections(),
                    parameter.getStoreName(),
                    parameter.getConnectionURL());

            while (uniProtKBAccessionIterator.hasNext()) {
                final UniProtKBAccession entry = uniProtKBAccessionIterator.next();
                Optional<UniProtKBEntry> result = client.getEntry(entry.getValue());
                if(result.isEmpty()){
                    throw new IndexDataStoreException("Unable to find entry: "+entry.getValue());
                }
            }
        }
    }

    private static class IterateUniParc implements VoidFunction<Iterator<UniParcId>> {

        private final DataStoreParameter parameter;

        public IterateUniParc(DataStoreParameter parameter){
            this.parameter = parameter;
        }

        @Override
        public void call(Iterator<UniParcId> uniParcEntryIdIterator) throws Exception {
            VoldemortClient<UniParcEntry> client = new VoldemortRemoteUniParcEntryStore(
                    parameter.getNumberOfConnections(),
                    parameter.getStoreName(),
                    parameter.getConnectionURL());

            while (uniParcEntryIdIterator.hasNext()) {
                final UniParcId entry = uniParcEntryIdIterator.next();
                Optional<UniParcEntry> result = client.getEntry(entry.getValue());
                if(result.isEmpty()){
                    throw new IndexDataStoreException("Unable to find entry: "+entry.getValue());
                }
            }
        }

    }


    private static class FlatFileToPrimaryAccession implements Function<String, UniProtKBAccession> {

        private static final long serialVersionUID = -4374937704856351325L;

        @Override
        public UniProtKBAccession call(String entryStr) throws Exception {
            final UniprotKBLineParser<AcLineObject> acParser =
                    new DefaultUniprotKBLineParserFactory().createAcLineParser();
            String[] lines = entryStr.split("\n");

            String acLine =
                    Arrays.stream(lines)
                            .filter(line -> line.startsWith("AC  "))
                            .collect(Collectors.joining("\n"));
            String accession = acParser.parse(acLine + "\n").primaryAcc;
            return new UniProtKBAccessionBuilder(accession).build();
        }
    }
}
