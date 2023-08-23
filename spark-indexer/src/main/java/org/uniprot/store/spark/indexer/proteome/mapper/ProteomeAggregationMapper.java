package org.uniprot.store.spark.indexer.proteome.mapper;

import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.proteome.ProteomeStatistics;

import scala.Tuple2;

public class ProteomeAggregationMapper
        implements Function2<
                JavaPairRDD<String, ProteomeStatistics>,
                Tuple2<Set<String>, ProteomeStatistics>,
                JavaPairRDD<String, ProteomeStatistics>> {
    @Override
    public JavaPairRDD<String, ProteomeStatistics> call(
            JavaPairRDD<String, ProteomeStatistics> v1, Tuple2<Set<String>, ProteomeStatistics> v2)
            throws Exception {
        return null;
    }
}
