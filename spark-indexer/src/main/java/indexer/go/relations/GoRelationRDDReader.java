package indexer.go.relations;

import indexer.util.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * @author lgonzales
 * @since 2019-11-09
 */
@Slf4j
public class GoRelationRDDReader {

    public static JavaPairRDD<String, GoTerm> load(ResourceBundle applicationConfig, JavaSparkContext sparkContext) {

        String goRelationsFolder = applicationConfig.getString("go.relations.dir.path");
        GoTermFileReader goTermFileReader = new GoTermFileReader(goRelationsFolder, sparkContext.hadoopConfiguration());
        List<GoTerm> goTerms = goTermFileReader.read();
        log.info("Loaded " + goTerms.size() + " GO relations terms");

        GoRelationFileReader goRelationFileReader = new GoRelationFileReader(goRelationsFolder, sparkContext.hadoopConfiguration());
        Map<String, Set<String>> relations = goRelationFileReader.read();
        log.info("Loaded " + relations.size() + " GO relations map");


        List<Tuple2<String, GoTerm>> pairs = new ArrayList<>();
        goTerms.stream().filter(Objects::nonNull).forEach(goTerm -> {
            Set<GoTerm> ancestors = getAncestors(goTerm, goTerms, relations);
            GoTerm goTermWithRelations = new GoTermImpl(goTerm.getId(), goTerm.getName(), ancestors);
            pairs.add(new Tuple2<String, GoTerm>(goTerm.getId(), goTermWithRelations));
        });
        log.info("Loaded  GO relations" + pairs.size());
        return (JavaPairRDD<String, GoTerm>) sparkContext.parallelizePairs(pairs);
    }


    private static Set<GoTerm> getAncestors(GoTerm term, List<GoTerm> goTerms, Map<String, Set<String>> relations) {
        Set<GoTerm> visited = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        queue.add(term.getId());
        visited.add(term);
        while (!queue.isEmpty()) {
            String vertex = queue.poll();
            for (String relatedGoId : relations.getOrDefault(vertex, Collections.emptySet())) {
                GoTerm relatedGoTerm = getGoTermById(relatedGoId, goTerms);
                if (!visited.contains(relatedGoTerm)) {
                    visited.add(relatedGoTerm);
                    queue.add(relatedGoId);
                }
            }
        }
        return visited;
    }

    private static GoTerm getGoTermById(String goTermId, List<GoTerm> goTerms) {
        GoTerm goTerm = new GoTermImpl(goTermId, null);
        if (goTerms.contains(goTerm)) {
            return goTerms.get(goTerms.indexOf(goTerm));
        } else {
            log.warn("GO TERM NOT FOUND FOR GO RELATION ID: " + goTermId);
            return goTerm;
        }
    }


    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        SparkConf sparkConf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master")).set("spark.driver.host", "localhost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, GoTerm> goRelations = GoRelationRDDReader.load(applicationConfig, sparkContext);

        System.out.println("COUNT:" + goRelations.count());
    }

}
