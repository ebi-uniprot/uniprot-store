package org.uniprot.store.spark.indexer.go.relations;

import static org.uniprot.store.spark.indexer.util.SparkUtils.getInputReleaseMainThreadDirPath;

import java.util.*;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.builder.GeneOntologyEntryBuilder;

import scala.Tuple2;

/**
 * This class load GoRelation to a JavaPairRDD{key=goTermId, value={@link GeneOntologyEntry with
 * Ancestors(Relations)}}
 *
 * @author lgonzales
 * @since 2019-11-09
 */
@Slf4j
public class GORelationRDDReader {

    /**
     * load GO Relations to a JavaPairRDD
     *
     * @return JavaPairRDD{key=goTermId, value={@link GeneOntologyEntry with Ancestors(Relations)}}
     */
    public static JavaPairRDD<String, GeneOntologyEntry> load(
            ResourceBundle applicationConfig, JavaSparkContext sparkContext, String releaseName) {

        String releaseInputDir = getInputReleaseMainThreadDirPath(applicationConfig, releaseName);
        String goRelationsFolder =
                releaseInputDir + applicationConfig.getString("go.relations.dir.path");
        GOTermFileReader goTermFileReader =
                new GOTermFileReader(goRelationsFolder, sparkContext.hadoopConfiguration());
        List<GeneOntologyEntry> goTerms = goTermFileReader.read();
        log.info("Loaded " + goTerms.size() + " GO relations terms");

        GORelationFileReader goRelationFileReader =
                new GORelationFileReader(goRelationsFolder, sparkContext.hadoopConfiguration());
        Map<String, Set<String>> relations = goRelationFileReader.read();
        log.info("Loaded " + relations.size() + " GO relations map");

        List<Tuple2<String, GeneOntologyEntry>> pairs = new ArrayList<>();
        goTerms.stream()
                .filter(Objects::nonNull)
                .forEach(
                        goTerm -> {
                            Set<GeneOntologyEntry> ancestors =
                                    getAncestors(goTerm, goTerms, relations);
                            GeneOntologyEntry goTermWithRelations =
                                    GeneOntologyEntryBuilder.from(goTerm)
                                            .ancestorsSet(ancestors)
                                            .build();
                            pairs.add(
                                    new Tuple2<String, GeneOntologyEntry>(
                                            goTerm.getId(), goTermWithRelations));
                        });
        log.info("Loaded  GO relations" + pairs.size());
        return (JavaPairRDD<String, GeneOntologyEntry>) sparkContext.parallelizePairs(pairs);
    }

    static Set<GeneOntologyEntry> getAncestors(
            GeneOntologyEntry term,
            List<GeneOntologyEntry> goTerms,
            Map<String, Set<String>> relations) {
        Set<GeneOntologyEntry> visited = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        queue.add(term.getId());
        visited.add(term);
        while (!queue.isEmpty()) {
            String vertex = queue.poll();
            for (String relatedGoId : relations.getOrDefault(vertex, Collections.emptySet())) {
                GeneOntologyEntry relatedGoTerm = getGoTermById(relatedGoId, goTerms);
                if (!visited.contains(relatedGoTerm)) {
                    visited.add(relatedGoTerm);
                    queue.add(relatedGoId);
                }
            }
        }
        return visited;
    }

    static GeneOntologyEntry getGoTermById(String goTermId, List<GeneOntologyEntry> goTerms) {
        GeneOntologyEntry goTerm = new GeneOntologyEntryBuilder().id(goTermId).build();
        if (goTerms.contains(goTerm)) {
            return goTerms.get(goTerms.indexOf(goTerm));
        } else {
            log.warn("GO TERM NOT FOUND FOR GO RELATION ID: " + goTermId);
            return goTerm;
        }
    }
}
