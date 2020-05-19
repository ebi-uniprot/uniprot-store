package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 17/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UniProtKBDocumentsToHDFSWriterTest {

    private JobParameter parameter;

    @BeforeAll
    void setUpWriter() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application);
        parameter =
                JobParameter.builder()
                        .applicationConfig(application)
                        .releaseName("2020_02")
                        .sparkContext(sparkContext)
                        .build();
    }

    @AfterAll
    void closeWriter() {
        parameter.getSparkContext().close();
    }

    @Test
    void joinGoRelations() {
        UniProtKBDocumentsToHDFSWriter writer = new UniProtKBDocumentsToHDFSWriter(parameter);
        JavaPairRDD<String, UniProtKBEntry> uniProtRDD =
                UniProtKBRDDTupleReader.load(parameter, false);

        JavaPairRDD<String, UniProtKBEntry> mergedUniProtRDD = writer.joinGoEvidences(uniProtRDD);
        assertNotNull(mergedUniProtRDD);
        long count = mergedUniProtRDD.count();
        assertEquals(1, count);
        Tuple2<String, UniProtKBEntry> tuple = mergedUniProtRDD.first();
        assertNotNull(tuple);
        assertEquals("Q9EPI6", tuple._1);

        UniProtKBEntry mergedEntry = tuple._2;

        assertEquals("Q9EPI6", mergedEntry.getPrimaryAccession().getValue());
    }

    @Test
    void joinAllUniRefs() {}

    @Test
    void joinGoEvidences() {}

    @Test
    void joinLiteratureMapped() {}
}
