package org.uniprot.store.spark.indexer.proteome.reader;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeStatisticsAggregationMapper;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeStatisticsMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

public class ProteomeStatisticsReader {
    private final UniProtKBRDDTupleReader uniProtKBReader;

    public ProteomeStatisticsReader(JobParameter parameter) {
        this.uniProtKBReader = new UniProtKBRDDTupleReader(parameter, false);
    }

    public JavaPairRDD<String, ProteomeStatistics> getProteomeStatisticsRDD() {
        JavaPairRDD<String, ProteomeStatistics> stringProteomeStatisticsJavaPairRDD = getProteinInfo()
                .flatMapToPair(getProteomeStatisticsMapper());
        System.out.println("****TOTAL_COUNT******"+stringProteomeStatisticsJavaPairRDD.count());
        stringProteomeStatisticsJavaPairRDD.take(200).forEach(t-> System.out.println("+++prot stat&&&:"+t._1+"~"+t._2.getReviewedProteinCount()
        +"**"+t._2.getUnreviewedProteinCount()+"**"+t._2.getIsoformProteinCount()));
        return stringProteomeStatisticsJavaPairRDD
                .aggregateByKey(
                        new ProteomeStatisticsBuilder()
                                .reviewedProteinCount(9999L)
                                .unreviewedProteinCount(73L)
                                .isoformProteinCount(112L)
                                .build(),
                        getProteomeStatisticsAggregationMapper(),
                        getProteomeStatisticsAggregationMapper());
    }

    JavaRDD<String> getProteinInfo() {
        return uniProtKBReader.loadFlatFileToRDD();
    }

    PairFlatMapFunction<String, String, ProteomeStatistics> getProteomeStatisticsMapper() {
        return new ProteomeStatisticsMapper();
    }

    Function2<ProteomeStatistics, ProteomeStatistics, ProteomeStatistics>
            getProteomeStatisticsAggregationMapper() {
        return new ProteomeStatisticsAggregationMapper();
    }
}
