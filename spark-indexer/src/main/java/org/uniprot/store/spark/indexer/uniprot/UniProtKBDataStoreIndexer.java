package org.uniprot.store.spark.indexer.uniprot;

import java.util.Iterator;
import java.util.ResourceBundle;

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
        ResourceBundle config = parameter.getApplicationConfig();
        UniProtKBRDDTupleReader uniprotkbReader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtKBEntry> uniprotRDD = uniprotkbReader.load();

        uniprotRDD = joinGoEvidences(uniprotRDD);
        uniprotRDD = joinUniParcId(uniprotRDD);


        String numberOfConnections = config.getString("store.uniprot.numberOfConnections");
        String storeName = config.getString("store.uniprot.storeName");
        String connectionURL = config.getString("store.uniprot.host");

        uniprotRDD
                .mapValues(new UniProtKBAnnotationScoreMapper())
                .values()
                .foreachPartition(getWriter(numberOfConnections, storeName, connectionURL));

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

    VoidFunction<Iterator<UniProtKBEntry>> getWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        return new UniProtKBDataStoreWriter(numberOfConnections, storeName, connectionURL);
    }
}
