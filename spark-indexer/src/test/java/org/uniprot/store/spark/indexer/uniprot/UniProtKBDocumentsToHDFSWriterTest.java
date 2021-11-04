package org.uniprot.store.spark.indexer.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtEntryToSolrDocument;

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

        String acc1 = "Q9EPI6";
        String acc2 = "P12345";
        String acc3 = "F7B8J7";

        tuples.add(new Tuple2<>(acc1, createUniProtDoc(acc1)));
        tuples.add(new Tuple2<>(acc2, createUniProtDoc(acc2)));
        tuples.add(new Tuple2<>(acc3, createUniProtDoc(acc3)));

        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                parameter.getSparkContext().parallelizePairs(tuples);
        uniprotDocRDD = writer.joinAllUniRefs(uniprotDocRDD);

        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(3, result.size());

        Map<String, List<UniProtDocument>> docMap = getResultMap(result, doc -> doc.accession);

        assertEquals("UniRef50_Q9EPI6", docMap.get(acc1).get(0).unirefCluster50);
        assertEquals("UniRef90_Q9EPI6", docMap.get(acc1).get(0).unirefCluster90);
        assertEquals("UniRef100_Q9EPI6", docMap.get(acc1).get(0).unirefCluster100);
        assertEquals("UPI00000E8551", docMap.get(acc1).get(0).uniparc);

        assertNull(docMap.get(acc2).get(0).unirefCluster50);
        assertNull(docMap.get(acc2).get(0).unirefCluster90);
        assertNull(docMap.get(acc2).get(0).uniparc);

        assertEquals("UniRef50_Q9EPI6", docMap.get(acc3).get(0).unirefCluster50);
        assertEquals("UniRef90_Q9EPI6", docMap.get(acc3).get(0).unirefCluster90);
        assertEquals("UPI0003ABCC0C", docMap.get(acc3).get(0).uniparc);
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
    void canJoinChebiRelations() {
        UniProtKBDocumentsToHDFSWriter writer = new UniProtKBDocumentsToHDFSWriter(parameter);

        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                reader.load().mapValues(new UniProtEntryToSolrDocument(new HashMap<>()));

        uniprotDocRDD = writer.joinChebiRelations(uniprotDocRDD);

        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(1, result.size());
        // TODO: Add validation
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

        assertEquals(1, result.get(0).computationalPubmedIds.size());
        assertEquals("1358782", result.get(0).computationalPubmedIds.get(0));

        assertEquals(2, result.get(0).communityPubmedIds.size());
        assertEquals("1358782", result.get(0).communityPubmedIds.get(0));
        assertEquals("5312045", result.get(0).communityPubmedIds.get(1));

        assertEquals(3, result.get(1).computationalPubmedIds.size());
        assertEquals("11203701", result.get(1).computationalPubmedIds.get(0));
        assertEquals("1358782", result.get(1).computationalPubmedIds.get(1));
        assertEquals("5312045", result.get(1).computationalPubmedIds.get(2));

        assertEquals(1, result.get(1).communityPubmedIds.size());
        assertEquals("1358782", result.get(0).communityPubmedIds.get(0));
    }

    private UniProtDocument createUniProtDoc(String accession) {
        UniProtDocument document = new UniProtDocument();
        document.accession = accession;
        return document;
    }

    private <T> Map<String, List<T>> getResultMap(
            List<T> result, Function<T, String> mappingFunction) {
        Map<String, List<T>> map = result.stream().collect(Collectors.groupingBy(mappingFunction));
        for (Map.Entry<String, List<T>> stringListEntry : map.entrySet()) {
            assertThat(stringListEntry.getValue(), hasSize(1));
        }
        return map;
    }
}
