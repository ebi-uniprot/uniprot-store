package org.uniprot.store.spark.indexer.chebi;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
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

        JavaRDD<Tuple2<Object, ChebiEntry>> vertices =
                jobParameter
                        .getSparkContext()
                        .textFile(filePath)
                        .filter(
                                input ->
                                        !input.startsWith("format-version")
                                                && !input.startsWith("[Typedef]"))
                        .map(new ChebiFileMapper());

        vertices = loadChebiGraph(vertices, MAX_GRAPH_LOAD_CYCLE);

        return vertices.mapToPair(new ChebiPairMapper());
    }

    JavaRDD<Tuple2<Object, ChebiEntry>> loadChebiGraph(
            JavaRDD<Tuple2<Object, ChebiEntry>> vertices, int maxCycle) {
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
