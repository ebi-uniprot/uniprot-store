package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
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
    void canJoinGoEvidences() {
        UniProtKBDocumentsToHDFSWriter writer = new UniProtKBDocumentsToHDFSWriter(parameter);
        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtKBEntry> uniProtRDD = reader.load();

        JavaPairRDD<String, UniProtKBEntry> mergedUniProtRDD = writer.joinGoEvidences(uniProtRDD);
        assertNotNull(mergedUniProtRDD);
        long count = mergedUniProtRDD.count();
        assertEquals(1, count);
        Tuple2<String, UniProtKBEntry> tuple = mergedUniProtRDD.first();
        assertNotNull(tuple);
        assertEquals("Q9EPI6", tuple._1);

        UniProtKBEntry mergedEntry = tuple._2;

        assertEquals("Q9EPI6", mergedEntry.getPrimaryAccession().getValue());

        List<UniProtKBCrossReference> goReferences =
                mergedEntry.getUniProtCrossReferencesByType("GO");
        assertNotNull(goReferences);

        UniProtKBCrossReference go5635 =
                goReferences.stream()
                        .filter(crossRef -> crossRef.getId().equals("GO:0005635"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);

        assertTrue(go5635.hasEvidences());
        assertEquals(1, go5635.getEvidences().size());

        UniProtKBCrossReference go5634 =
                goReferences.stream()
                        .filter(crossRef -> crossRef.getId().equals("GO:0005634"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);

        assertTrue(go5634.hasEvidences());
        assertEquals(3, go5634.getEvidences().size());
    }

    @Test
    void canJoinAllUniRefs() {
        UniProtKBDocumentsToHDFSWriter writer = new UniProtKBDocumentsToHDFSWriter(parameter);

        List<Tuple2<String, UniProtDocument>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>("Q9EPI6", new UniProtDocument()));
        tuples.add(new Tuple2<>("P12345", new UniProtDocument()));
        tuples.add(new Tuple2<>("F7B8J7", new UniProtDocument()));

        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                parameter.getSparkContext().parallelizePairs(tuples);
        uniprotDocRDD = writer.joinAllUniRefs(uniprotDocRDD);

        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("UniRef50_Q9EPI6", result.get(0).unirefCluster50);
        assertEquals("UniRef90_Q9EPI6", result.get(0).unirefCluster90);
        assertEquals("UniRef100_Q9EPI6", result.get(0).unirefCluster100);
        assertEquals("UPI00000E8551", result.get(0).uniparc);

        assertNull(result.get(1).unirefCluster50);
        assertNull(result.get(1).unirefCluster90);
        assertNull(result.get(1).uniparc);

        assertEquals("UniRef50_Q9EPI6", result.get(2).unirefCluster50);
        assertEquals("UniRef90_Q9EPI6", result.get(2).unirefCluster90);
        assertEquals("UPI0003ABCC0C", result.get(2).uniparc);
    }

    @Test
    void canJoinGoRelations() {
        UniProtKBDocumentsToHDFSWriter writer = new UniProtKBDocumentsToHDFSWriter(parameter);

        List<Tuple2<String, UniProtDocument>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>("Q9EPI6", new UniProtDocument()));
        tuples.add(new Tuple2<>("P21802", new UniProtDocument()));

        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                parameter.getSparkContext().parallelizePairs(tuples);
        uniprotDocRDD = writer.joinGoRelations(uniprotDocRDD);
        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(2, result.size());

        assertEquals(8, result.get(0).goIds.size());
        assertTrue(result.get(0).goIds.contains("0016765"));
        assertTrue(result.get(0).goIds.contains("0007005"));
        assertEquals(14, result.get(0).goes.size());
        assertTrue(result.get(0).goes.contains("mitochondrion organization"));
        assertTrue(result.get(0).goes.contains("0030863"));

        assertEquals(0, result.get(1).goIds.size());
        assertEquals(0, result.get(1).goes.size());
    }

    @Test
    void canJoinLiteratureMapped() {
        UniProtKBDocumentsToHDFSWriter writer = new UniProtKBDocumentsToHDFSWriter(parameter);

        List<Tuple2<String, UniProtDocument>> tuples = new ArrayList<>();
        tuples.add(new Tuple2<>("P21802", new UniProtDocument()));
        tuples.add(new Tuple2<>("B5U9V4", new UniProtDocument()));

        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                parameter.getSparkContext().parallelizePairs(tuples);
        uniprotDocRDD = writer.joinLiteratureMapped(uniprotDocRDD);
        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(2, result.size());

        assertEquals(1, result.get(0).mappedCitation.size());
        assertEquals("1358782", result.get(0).mappedCitation.get(0));

        assertEquals(3, result.get(1).mappedCitation.size());
        assertEquals("11203701", result.get(1).mappedCitation.get(0));
        assertEquals("1358782", result.get(1).mappedCitation.get(1));
        assertEquals("5312045", result.get(1).mappedCitation.get(2));
    }
}
