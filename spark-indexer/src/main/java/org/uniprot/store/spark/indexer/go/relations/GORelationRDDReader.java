package org.uniprot.store.spark.indexer.go.relations;

import static org.uniprot.store.spark.indexer.util.SparkUtils.getInputReleaseDirPath;

import java.util.*;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class load GoRelation to a JavaPairRDD{key=goTermId, value={@link GOTerm with
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
     * @return JavaPairRDD{key=goTermId, value={@link GOTerm with Ancestors(Relations)}}
     */
    public static JavaPairRDD<String, GOTerm> load(
            ResourceBundle applicationConfig, JavaSparkContext sparkContext, String releaseName) {

        String releaseInputDir = getInputReleaseDirPath(applicationConfig, releaseName);
        String goRelationsFolder =
                releaseInputDir + applicationConfig.getString("go.relations.dir.path");
        GOTermFileReader goTermFileReader =
                new GOTermFileReader(goRelationsFolder, sparkContext.hadoopConfiguration());
        List<GOTerm> goTerms = goTermFileReader.read();
        log.info("Loaded " + goTerms.size() + " GO relations terms");

        GORelationFileReader goRelationFileReader =
                new GORelationFileReader(goRelationsFolder, sparkContext.hadoopConfiguration());
        Map<String, Set<String>> relations = goRelationFileReader.read();
        log.info("Loaded " + relations.size() + " GO relations map");

        List<Tuple2<String, GOTerm>> pairs = new ArrayList<>();
        goTerms.stream()
                .filter(Objects::nonNull)
                .forEach(
                        goTerm -> {
                            Set<GOTerm> ancestors = getAncestors(goTerm, goTerms, relations);
                            GOTerm goTermWithRelations =
                                    new GOTermImpl(goTerm.getId(), goTerm.getName(), ancestors);
                            pairs.add(
                                    new Tuple2<String, GOTerm>(
                                            goTerm.getId(), goTermWithRelations));
                        });
        log.info("Loaded  GO relations" + pairs.size());
        return (JavaPairRDD<String, GOTerm>) sparkContext.parallelizePairs(pairs);
    }

    static Set<GOTerm> getAncestors(
            GOTerm term, List<GOTerm> goTerms, Map<String, Set<String>> relations) {
        Set<GOTerm> visited = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        queue.add(term.getId());
        visited.add(term);
        while (!queue.isEmpty()) {
            String vertex = queue.poll();
            for (String relatedGoId : relations.getOrDefault(vertex, Collections.emptySet())) {
                GOTerm relatedGoTerm = getGoTermById(relatedGoId, goTerms);
                if (!visited.contains(relatedGoTerm)) {
                    visited.add(relatedGoTerm);
                    queue.add(relatedGoId);
                }
            }
        }
        return visited;
    }

    static GOTerm getGoTermById(String goTermId, List<GOTerm> goTerms) {
        GOTerm goTerm = new GOTermImpl(goTermId, null);
        if (goTerms.contains(goTerm)) {
            return goTerms.get(goTerms.indexOf(goTerm));
        } else {
            log.warn("GO TERM NOT FOUND FOR GO RELATION ID: " + goTermId);
            return goTerm;
        }
    }
}
