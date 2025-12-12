package org.uniprot.store.spark.indexer.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtEntryToSolrDocument;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 17/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UniProtKBDocumentsToHPSWriterTest {

    private JobParameter parameter;

    @BeforeAll
    void setUpWriter() {
        Config application = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER);
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
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);
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
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);

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
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);

        List<Tuple2<String, UniProtDocument>> tuples = new ArrayList<>();
        String accession1 = "Q9EPI6";
        String accession2 = "P21802";
        UniProtDocument doc1 = new UniProtDocument();
        doc1.accession = accession1;
        UniProtDocument doc2 = new UniProtDocument();
        doc2.accession = accession2;
        tuples.add(new Tuple2<>(accession1, doc1));
        tuples.add(new Tuple2<>(accession2, doc2));

        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                parameter.getSparkContext().parallelizePairs(tuples);
        uniprotDocRDD = writer.joinGoRelations(uniprotDocRDD);
        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(2, result.size());
        Optional<UniProtDocument> joinedDoc1 =
                result.stream().filter(doc -> accession1.equals(doc.accession)).findFirst();
        Optional<UniProtDocument> joinedDoc2 =
                result.stream().filter(doc -> accession2.equals(doc.accession)).findFirst();
        assertTrue(joinedDoc1.isPresent());
        assertTrue(joinedDoc2.isPresent());

        assertEquals(8, joinedDoc1.get().goIds.size());
        assertTrue(joinedDoc1.get().goIds.contains("0016765"));
        assertTrue(joinedDoc1.get().goIds.contains("0007005"));
        assertEquals(14, joinedDoc1.get().goes.size());
        assertTrue(joinedDoc1.get().goes.contains("mitochondrion organization"));
        assertTrue(joinedDoc1.get().goes.contains("0030863"));

        assertEquals(0, joinedDoc2.get().goIds.size());
        assertEquals(0, joinedDoc2.get().goes.size());
    }

    @Test
    void canJoinChebiRelations() {
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);

        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                reader.load()
                        .mapValues(
                                new UniProtEntryToSolrDocument(new HashMap<>(), new HashMap<>()));

        uniprotDocRDD = writer.joinChebiRelations(uniprotDocRDD);

        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(1, result.size());
        // TODO: Add validation
    }

    @Test
    void canJoinUniProtOldIdTracker() {
        List<String> mappedResult =
                Arrays.asList("NSMF_RAT", "NSMFOLD1_RAT", "NSMFOLD3_RAT", "NSMFOLD2_RAT");
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);

        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                reader.load()
                        .mapValues(
                                new UniProtEntryToSolrDocument(new HashMap<>(), new HashMap<>()));

        uniprotDocRDD = writer.joinUniProtOldIdTracker(uniprotDocRDD);

        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(1, result.size());
        UniProtDocument doc = result.get(0);
        assertEquals(4, doc.id.size());
        assertEquals(mappedResult, doc.id);
        assertEquals(4, doc.idDefault.size());
        assertEquals(mappedResult, doc.idDefault);
        assertFalse(doc.content.containsAll(mappedResult));
    }

    @Test
    void canJoinLiteratureMapped() {
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);

        List<Tuple2<String, UniProtDocument>> tuples = new ArrayList<>();

        String accession1 = "P21802";
        String accession2 = "B5U9V4";
        UniProtDocument doc1 = new UniProtDocument();
        doc1.accession = accession1;
        UniProtDocument doc2 = new UniProtDocument();
        doc2.accession = accession2;
        tuples.add(new Tuple2<>(accession1, doc1));
        tuples.add(new Tuple2<>(accession2, doc2));

        JavaPairRDD<String, UniProtDocument> uniprotDocRDD =
                parameter.getSparkContext().parallelizePairs(tuples);
        uniprotDocRDD = writer.joinLiteratureMapped(uniprotDocRDD);
        List<UniProtDocument> result = uniprotDocRDD.values().take(10);
        assertNotNull(result);
        assertEquals(2, result.size());
        Optional<UniProtDocument> optJoinedDoc1 =
                result.stream().filter(doc -> accession1.equals(doc.accession)).findFirst();
        Optional<UniProtDocument> optJoinedDoc2 =
                result.stream().filter(doc -> accession2.equals(doc.accession)).findFirst();
        assertTrue(optJoinedDoc1.isPresent());
        assertTrue(optJoinedDoc2.isPresent());

        assertEquals(1, optJoinedDoc2.get().computationalPubmedIds.size());
        assertEquals("1358782", optJoinedDoc2.get().computationalPubmedIds.get(0));

        assertEquals(2, optJoinedDoc2.get().communityPubmedIds.size());
        assertEquals("1358782", optJoinedDoc2.get().communityPubmedIds.get(0));
        assertEquals("5312045", optJoinedDoc2.get().communityPubmedIds.get(1));

        assertEquals(3, optJoinedDoc2.get().referenceCitationIds.size());
        assertEquals("1358782", optJoinedDoc2.get().referenceCitationIds.get(0));
        assertEquals("1358782", optJoinedDoc2.get().referenceCitationIds.get(1));
        assertEquals("5312045", optJoinedDoc2.get().referenceCitationIds.get(2));

        assertEquals(3, optJoinedDoc1.get().computationalPubmedIds.size());
        assertEquals("11203701", optJoinedDoc1.get().computationalPubmedIds.get(0));
        assertEquals("1358782", optJoinedDoc1.get().computationalPubmedIds.get(1));
        assertEquals("5312045", optJoinedDoc1.get().computationalPubmedIds.get(2));

        assertEquals(1, optJoinedDoc1.get().communityPubmedIds.size());
        assertEquals("1358782", optJoinedDoc1.get().communityPubmedIds.get(0));

        assertEquals(4, optJoinedDoc1.get().referenceCitationIds.size());
        assertEquals("11203701", optJoinedDoc1.get().referenceCitationIds.get(0));
        assertEquals("1358782", optJoinedDoc1.get().referenceCitationIds.get(1));
        assertEquals("1358782", optJoinedDoc1.get().referenceCitationIds.get(2));
        assertEquals("5312045", optJoinedDoc1.get().referenceCitationIds.get(3));
    }

    @Test
    void canJoinSubcellularLocation() {
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);
        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtKBEntry> uniProtRDD = reader.load();
        JavaPairRDD<String, UniProtDocument> uniProtDocument =
                uniProtRDD.mapValues(
                        new UniProtEntryToSolrDocument(new HashMap<>(), new HashMap<>()));
        Map<String, UniProtDocument> accessionDoc = uniProtDocument.collectAsMap();
        assertNotNull(accessionDoc);
        assertEquals(1, accessionDoc.size());
        UniProtDocument documentBeforeJoin = accessionDoc.values().stream().findFirst().get();
        List<String> slIds =
                documentBeforeJoin.content.stream()
                        .filter(str -> str.startsWith("SL-"))
                        .collect(Collectors.toList());
        Set<String> slTerms = documentBeforeJoin.subcellLocationTerm;
        assertEquals(13, slIds.size());
        assertEquals(26, documentBeforeJoin.subcellLocationTerm.size());
        assertTrue(documentBeforeJoin.subcellLocationTerm.containsAll(slIds));
        // verify the number of sl ids in content and subcell term
        JavaPairRDD<String, UniProtDocument> uniProtDocumentWithSubcells =
                writer.joinSubcellularLocationRelations(uniProtRDD, uniProtDocument);
        // after join
        Map<String, UniProtDocument> accessionDocAfterJoin =
                uniProtDocumentWithSubcells.collectAsMap();
        assertNotNull(accessionDocAfterJoin);
        assertEquals(1, accessionDocAfterJoin.size());
        UniProtDocument documentAfterJoin =
                accessionDocAfterJoin.values().stream().findFirst().get();
        List<String> slIdsAfterJoin =
                documentAfterJoin.content.stream()
                        .filter(str -> str.startsWith("SL-"))
                        .collect(Collectors.toList());
        assertEquals(19, slIdsAfterJoin.size());
        assertTrue(slIdsAfterJoin.containsAll(slIds));
        assertEquals(44, documentAfterJoin.subcellLocationTerm.size());
        assertTrue(documentAfterJoin.subcellLocationTerm.containsAll(slTerms));
    }

    @Test
    void canGetInactiveEntries() {
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);
        JavaPairRDD<String, UniProtDocument> inactiveRDD = writer.getInactiveEntryRDD();
        assertNotNull(inactiveRDD);
        List<Tuple2<String, UniProtDocument>> inactiveEntries = inactiveRDD.collect();
        assertFalse(inactiveEntries.isEmpty());
        assertEquals(23L, inactiveEntries.size());
        Tuple2<String, UniProtDocument> inactive =
                inactiveEntries.stream()
                        .filter(tuple2 -> tuple2._1.equals("I8FBX0"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        validateInactiveEntry(inactive, "I8FBX0", "DELETED", "UPI00000E8551");

        inactive =
                inactiveEntries.stream()
                        .filter(tuple2 -> tuple2._1.equals("I8FBX1"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        validateInactiveEntry(inactive, "I8FBX1", "DELETED:PROTEOME_REDUNDANCY", "UPI000000017F");

        inactive =
                inactiveEntries.stream()
                        .filter(tuple2 -> tuple2._1.equals("I8FBX2"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        validateInactiveEntry(inactive, "I8FBX2", "DELETED", "UPI00000E8551");

        inactive =
                inactiveEntries.stream()
                        .filter(tuple2 -> tuple2._1.equals("Q00015"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        validateInactiveEntry(inactive, "Q00015", "MERGED:P23141", null);

        inactive =
                inactiveEntries.stream()
                        .filter(tuple2 -> tuple2._1.equals("Q00007"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        validateInactiveEntry(inactive, "Q00007", "DEMERGED:P63150,P63151", "UPI000000017F");
    }

    @Test
    void canLoadDiseases() {
        UniProtKBDocumentsToHPSWriter writer = new UniProtKBDocumentsToHPSWriter(parameter);
        Map<String, DiseaseEntry> diseaseIdEntryMap =
                writer.loadDiseases(
                        parameter.getSparkContext().hadoopConfiguration(),
                        parameter.getReleaseName());
        assertNotNull(diseaseIdEntryMap);
        assertEquals(4, diseaseIdEntryMap.size());
    }

    private static void validateInactiveEntry(
            Tuple2<String, UniProtDocument> tuple,
            String accession,
            String inactiveReason,
            String uniparcId) {
        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertNotNull(tuple._2);
        assertEquals(accession, tuple._1);
        UniProtDocument obsoleteDoc = tuple._2;
        assertFalse(obsoleteDoc.active);
        assertEquals(accession, obsoleteDoc.accession);
        assertEquals(inactiveReason, obsoleteDoc.inactiveReason);
        assertEquals(uniparcId, obsoleteDoc.deletedEntryUniParc);
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
