package org.uniprot.store.spark.indexer.chebi;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.chebi.mapper.*;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * ChebiRDDReader loads CHEBI data and also its related ids "is_a","is_conjugate_base_of" and
 * "has_major_microspecies_at_pH_7_3" from chebi.obo file. It also loads extra relatedIds from
 * chebi_pH7_3_mapping.tsv file. Chebi data is a graph structure, and we use Apache Spark GraphX
 * library to load it.
 *
 * <p>Bellow is the link that explain how Apache Spark GraphX works, and also gives more detail
 * about the pregel algorithm, that we use build the complete graph with all related ids.
 * https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api
 *
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiRDDReader implements PairRDDReader<String, ChebiEntry> {

    private static final int MAX_GRAPH_LOAD_CYCLE = 10;
    private static final int MAX_PREGEL_CYCLE = 1;
    private static final int MIN_GRAPH_CYCLE =
            4; // from tests, we know that currently we have 6 cycles
    private final JobParameter jobParameter;

    private static final Logger logger = LoggerFactory.getLogger(ChebiRDDReader.class);

    public ChebiRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return JavaPairRDD{key=chebiId, value={@link ChebiEntry}} */
    @Override
    public JavaPairRDD<String, ChebiEntry> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("chebi.file.path");

        jobParameter
                .getSparkContext()
                .hadoopConfiguration()
                .set("textinputformat.record.delimiter", "\n\n");

        JavaPairRDD<Long, ChebiEntry> chebiRDD = loadChebiRDD(filePath);

        // JavaPairRDD<chebiId, Iterable<relatedChebiEntry>>
        JavaPairRDD<Long, Iterable<ChebiEntry>> relatedChebi = loadRelatedIdsRDD(chebiRDD);

        // JavaPairRDD<chebiId, ChebiEntry>
        JavaRDD<Tuple2<Object, ChebiEntry>> vertices =
                chebiRDD.leftOuterJoin(relatedChebi)
                        .values() // Tuple2<ChebiEntry, Optional<Iterable<ChebiRelatedEntry>>>
                        .map(new ChebiRelatedIdsJoinMapper());

        vertices = loadChebiGraph(vertices, MAX_GRAPH_LOAD_CYCLE);

        return vertices.mapToPair(new ChebiPairMapper());
    }

    /**
     * This method load all related ids for a chebi entry.
     *
     * @param chebiRDD chebiRDD to extract related ids.
     * @return JavaPairRDD<chebiId,Iterable<ChebiRelatedEntry>>
     */
    private JavaPairRDD<Long, Iterable<ChebiEntry>> loadRelatedIdsRDD(
            JavaPairRDD<Long, ChebiEntry> chebiRDD) {
        // JavaPairRDD<relatedId,chebiId>
        JavaPairRDD<Long, Long> relatedIdRdd =
                chebiRDD.values().flatMapToPair(new ChebiRelatedIdsMapper());

        return chebiRDD.join(relatedIdRdd) // Tuple2<relatedId, Tuple2<RelatedChebiEntry, chebiId>>>
                .mapToPair(new ChebiRelatedChebiMapper()) // Tuple2<ChebiId, RelatedChebiEntry>
                .groupByKey(); //  JavaPairRDD<chebiId,Iterable<ChebiRelatedEntry>>
    }

    /**
     * This method load chebi.obo file into JavaPairRDD of chebi entries.
     *
     * @param filePath chebi.obo file path (extracted from application.properties)
     * @return return JavaPairRDD<chebiId,ChebiEntry>
     */
    private JavaPairRDD<Long, ChebiEntry> loadChebiRDD(String filePath) {
        JavaPairRDD<Long, ChebiEntry> chebiRDD =
                jobParameter
                        .getSparkContext()
                        .textFile(filePath)
                        .filter(
                                input ->
                                        !input.startsWith("format-version")
                                                && !input.startsWith("[Typedef]"))
                        .mapToPair(new ChebiFileMapper());
        logger.info("Chebi RDD Count: " + chebiRDD.count());
        logger.info("Chebi RDD Related Ids sum: " + chebiRDD.values().map(entry -> entry.getRelatedIds().size()).reduce( (value1, value2) -> value1 + value2));
        logger.info("Chebi RDD Major Microspecies sum: " + chebiRDD.values().map(entry -> entry.getMajorMicrospecies().size()).reduce( (value1, value2) -> value1 + value2));
        return chebiRDD;
    }

    /**
     * This method load Chebi Graph. Important that we have a maxCycle break to avoid infinite loop
     * (graph cycle). We also have a MIN_GRAPH_CYCLE, it means that we only check Graph load
     * completion after this MIN_GRAPH_CYCLE was loaded (to optimise load).
     *
     * @param vertices loaded graph vertices
     * @param maxCycle maxCycle break to avoid infinite loop (graph cycle)
     * @return complete loaded chebi graph
     */
    JavaRDD<Tuple2<Object, ChebiEntry>> loadChebiGraph(
            JavaRDD<Tuple2<Object, ChebiEntry>> vertices, final int maxCycle) {
        boolean shouldRunAgain = true;
        Integer idWithRelatedCount =
                vertices.map(ChebiRDDReader::countRelatedId)
                        .aggregate(0, ChebiRDDReader::sum, ChebiRDDReader::sum);
        for (int i = 0; i <= maxCycle && shouldRunAgain; i++) {
            JavaRDD<Edge<String>> relationships = mapEdgesFromVertice(vertices);
            vertices = executePregel(vertices, relationships);
            if (i > MIN_GRAPH_CYCLE) {
                int idWithRelatedCountAfterPregel =
                        vertices.map(ChebiRDDReader::countRelatedId)
                                .aggregate(0, ChebiRDDReader::sum, ChebiRDDReader::sum);
                if (completeGraphLoaded(idWithRelatedCount, idWithRelatedCountAfterPregel)) {
                    shouldRunAgain = false;
                } else if (i < maxCycle) {
                    idWithRelatedCount = idWithRelatedCountAfterPregel;
                } else {
                    throw new SparkIndexException(
                            "Unable to finish loading CHEBI graph before reach MAX_GRAPH_LOAD_CYCLE. IdsAndRelatedIds before last cycle:"
                                    + idWithRelatedCount
                                    + " after: "
                                    + idWithRelatedCountAfterPregel);
                }
            }
        }
        return vertices;
    }

    /**
     * This method execute the Spark GraphX pregel method, that is an algorithm to navigate through
     * a distributed graph
     *
     * <p>graph.ops().pregel: takes two argument lists (i.e., graph.pregel(list1)(list2)). The first
     * argument list contains configuration parameters including the initial message, the maximum
     * number of iterations, and the edge direction in which to send messages (by default along out
     * edges). The second argument list contains the user defined functions for receiving messages
     * (the vertex program vprog), computing messages (sendMsg), and combining messages mergeMsg.
     *
     * @param vertices
     * @param relationships
     * @return
     */
    private JavaRDD<Tuple2<Object, ChebiEntry>> executePregel(
            JavaRDD<Tuple2<Object, ChebiEntry>> vertices, JavaRDD<Edge<String>> relationships) {
        ClassTag<ChebiEntry> chebiTag = scala.reflect.ClassTag$.MODULE$.apply(ChebiEntry.class);
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

        Graph<ChebiEntry, String> graph =
                Graph.apply(
                        JavaRDD.toRDD(vertices),
                        JavaRDD.toRDD(relationships),
                        null,
                        StorageLevel.DISK_ONLY(),
                        StorageLevel.DISK_ONLY(),
                        chebiTag,
                        stringTag);

        return graph.ops()
                .pregel(
                        null,
                        MAX_PREGEL_CYCLE,
                        EdgeDirection.Out(),
                        new GraphVerticesProgramMapper(),
                        new GraphSendVertexMessageMapper(),
                        new GraphMergeVertexMapper(),
                        chebiTag)
                .vertices()
                .toJavaRDD();
    }

    private static boolean completeGraphLoaded(
            Integer idWithRelatedCounts, int idWithRelatedCountsAfterPregel) {
        return idWithRelatedCounts == idWithRelatedCountsAfterPregel;
    }

    private static JavaRDD<Edge<String>> mapEdgesFromVertice(
            JavaRDD<Tuple2<Object, ChebiEntry>> vertices) {
        return vertices.flatMap(new GraphExtractEdgesFromVerticesMapper());
    }

    private static Integer sum(Integer int1, Integer integer) {
        return integer + int1;
    }

    private static int countRelatedId(Tuple2<Object, ChebiEntry> tuple) {
        return tuple._2.getRelatedIds().size() + 1;
    }
}
