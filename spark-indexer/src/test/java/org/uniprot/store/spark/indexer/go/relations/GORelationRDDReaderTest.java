package org.uniprot.store.spark.indexer.go.relations;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GORelationRDDReaderTest {

    @Test
    void testLoadGORelation() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            GORelationRDDReader reader = new GORelationRDDReader(parameter);
            JavaPairRDD<String, GeneOntologyEntry> goRelationRDD = reader.load();
            assertNotNull(goRelationRDD);
            long count = goRelationRDD.count();
            assertEquals(12L, count);
            Tuple2<String, GeneOntologyEntry> tuple =
                    goRelationRDD.filter(tuple2 -> tuple2._1.equals("GO:0000001")).first();

            assertNotNull(tuple);
            assertEquals("GO:0000001", tuple._1);
            assertEquals("mitochondrion inheritance", tuple._2.getName());
        }
    }

    @Test
    void getAncestorsWithAncestors() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = new ArrayList<>();
        goTermList.add(goTerm1);
        goTermList.add(go("GO2", "TERM2"));
        goTermList.add(go("GO3", "TERM3"));
        goTermList.add(go("GO4", "TERM4"));
        goTermList.add(go("GO5", "TERM5"));
        Map<String, Set<String>> relations = new HashMap<>();
        relations.put("GO1", Collections.singleton("GO3"));

        Set<String> go3Relations = new HashSet<>();
        go3Relations.add("GO4");
        go3Relations.add("GO5");
        go3Relations.add("GO1");
        relations.put("GO3", go3Relations);

        GORelationRDDReader reader = new GORelationRDDReader(null);
        Set<GeneOntologyEntry> goTermRelations =
                reader.getAncestors(goTerm1, goTermList, relations);
        assertNotNull(goTermRelations);
        assertEquals(4, goTermRelations.size());
        assertTrue(goTermRelations.contains(go("GO1", null)));
        assertTrue(goTermRelations.contains(go("GO3", null)));
        assertTrue(goTermRelations.contains(go("GO4", null)));
        assertTrue(goTermRelations.contains(go("GO5", null)));
    }

    @Test
    void getAncestorsWithoutAncestors() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = Collections.singletonList(goTerm1);
        GORelationRDDReader reader = new GORelationRDDReader(null);
        Set<GeneOntologyEntry> goTermRelations =
                reader.getAncestors(goTerm1, goTermList, new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(go("GO1", null)));
    }

    @Test
    void getAncestorsWithoutGoTermAndAncertors() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        GORelationRDDReader reader = new GORelationRDDReader(null);
        Set<GeneOntologyEntry> goTermRelations =
                reader.getAncestors(goTerm1, new ArrayList<>(), new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(go("GO1", null)));
    }

    @Test
    void getValidGoTermById() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = Collections.singletonList(goTerm1);
        GORelationRDDReader reader = new GORelationRDDReader(null);
        GeneOntologyEntry result = reader.getGoTermById("GO1", goTermList);

        assertNotNull(result);
        assertEquals(goTerm1, result);
    }

    @Test
    void getInValidGoTermById() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = Collections.singletonList(goTerm1);
        GORelationRDDReader reader = new GORelationRDDReader(null);
        GeneOntologyEntry result = reader.getGoTermById("GO2", goTermList);

        assertNotNull(result);
        assertEquals("GO2", result.getId());
        assertNull(result.getName());
    }

    private GeneOntologyEntry go(String id, String name) {
        return new GeneOntologyEntryBuilder().id(id).name(name).build();
    }
}
