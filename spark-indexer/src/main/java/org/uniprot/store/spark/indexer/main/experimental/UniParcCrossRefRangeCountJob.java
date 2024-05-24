package org.uniprot.store.spark.indexer.main.experimental;

import java.util.*;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniparc.UniParcRDDTupleReader;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class UniParcCrossRefRangeCountJob {

    private final JobParameter parameter;

    public UniParcCrossRefRangeCountJob(JobParameter parameter) {
        this.parameter = parameter;
    }

    public void findMostRepeatedTaxonomy() {
        // find entries with more than 1000 cross refs
        // group by active taxonomy id
        // then pick maximum repetition count for each taxonomy
        // sort by count in descending order and return first 500
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniParcRDD = reader.load();
        // group by taxonomy id
        Function<UniParcEntry, Boolean> entryWithMoreXrefs =
                entry -> entry.getUniParcCrossReferences().size() > 1000;
        JavaRDD<Iterable<Tuple2<String, Tuple2<Long, Long>>>> uniParcEntry1000PlusXrefRDD =
                uniParcRDD.filter(entryWithMoreXrefs).map(new UniParcGroupByTaxonomyId());
        // flatten the iterable to have <uniparcid, <taxonId, count>>
        JavaRDD<Tuple2<String, Tuple2<Long, Long>>> flattenedUniParcRDD =
                uniParcEntry1000PlusXrefRDD.flatMap(
                        iterable -> {
                            List<Tuple2<String, Tuple2<Long, Long>>> flattenedList =
                                    new ArrayList<>();
                            for (Tuple2<String, Tuple2<Long, Long>> tuple : iterable) {
                                flattenedList.add(tuple);
                            }
                            return flattenedList.iterator();
                        });
        // Step 1: Convert to PairRDD with taxonomy as key and the original Tuple as value i.e.
        // <taxonId, <uniparcid, <taxonId, count>>>
        JavaPairRDD<Long, Tuple2<String, Tuple2<Long, Long>>> pairRDD =
                flattenedUniParcRDD.mapToPair(tuple -> new Tuple2<>(tuple._2()._1(), tuple));

        // Step 2: Group by the first taxonomy
        JavaPairRDD<Long, Iterable<Tuple2<String, Tuple2<Long, Long>>>> groupedRDD =
                pairRDD.groupByKey();

        // Step 3: Within each group, pick the Tuple with the largest count
        JavaRDD<Tuple2<String, Tuple2<Long, Long>>> maxInGroupRDD =
                groupedRDD
                        .mapValues(
                                iterable ->
                                        StreamSupport.stream(iterable.spliterator(), false)
                                                .max(Comparator.comparing(tuple -> tuple._2()._2()))
                                                .get())
                        .values();

        // Step 4: Sort by the count in descending order
        JavaRDD<Tuple2<String, Tuple2<Long, Long>>> sortedUniParcRDD =
                maxInGroupRDD.sortBy(tuple -> tuple._2()._2(), false, 1);
        // Step 5: Take top 500 and print them
        List<Tuple2<String, Tuple2<Long, Long>>> top500CrossRefs = sortedUniParcRDD.take(500);
        log.info("Top 500 taxonomies repeated most number of times");
        log.info("UniParcId \t TaxonomyId \t Repetition");
        for (Tuple2<String, Tuple2<Long, Long>> tuple : top500CrossRefs) {
            log.info("{} \t {} \t {}", tuple._1, tuple._2._1, tuple._2._2);
        }
    }

    public void countMostRepeatedTaxonomyIdByUniRefId() {
        // find entries with more than 1000 cross refs
        // group by active taxonomy id
        // sort by count in descending order and return first 500
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniParcRDD = reader.load();
        // group by taxonomy id
        Function<UniParcEntry, Boolean> entryWithMoreXrefs =
                entry -> entry.getUniParcCrossReferences().size() > 1000;
        JavaRDD<Iterable<Tuple2<String, Tuple2<Long, Long>>>> uniParcEntry1000PlusXrefRDD =
                uniParcRDD.filter(entryWithMoreXrefs).map(new UniParcGroupByTaxonomyId());
        // flatten the iterable to have <uniparcid, <taxonId, count>>
        JavaRDD<Tuple2<String, Tuple2<Long, Long>>> flattenedUniParcRDD =
                uniParcEntry1000PlusXrefRDD.flatMap(
                        iterable -> {
                            List<Tuple2<String, Tuple2<Long, Long>>> flattenedList =
                                    new ArrayList<>();
                            for (Tuple2<String, Tuple2<Long, Long>> tuple : iterable) {
                                flattenedList.add(tuple);
                            }
                            return flattenedList.iterator();
                        });
        // sort by repetition count of taxonomy id in descending order
        JavaRDD<Tuple2<String, Tuple2<Long, Long>>> sortedUniParcRDD =
                flattenedUniParcRDD.sortBy(
                        (Function<Tuple2<String, Tuple2<Long, Long>>, Long>) tuple -> tuple._2._2,
                        false,
                        1);
        // take top 500 and print them
        List<Tuple2<String, Tuple2<Long, Long>>> top500CrossRefs = sortedUniParcRDD.take(500);
        log.info("Top 500 taxonomies repeated most number of times");
        log.info("UniParcId \t TaxonomyId \t Repetition");
        for (Tuple2<String, Tuple2<Long, Long>> tuple : top500CrossRefs) {
            log.info("{} \t {} \t {}", tuple._1, tuple._2._1, tuple._2._2);
        }
    }

    // cross ref range -> uniparc entry count. e.g. 1000-2000 --> 200
    public void countCrossRefByRange() {
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniParcRDD = reader.load();

        log.info("Total UniParc Entry count in input file {}", uniParcRDD.count());

        JavaPairRDD<String, Integer> idCount =
                uniParcRDD
                        .mapToPair(
                                entry ->
                                        new Tuple2<>(
                                                entry.getUniParcId().getValue(),
                                                entry.getUniParcCrossReferences().size()))
                        .aggregateByKey(
                                null,
                                (e1, e2) -> e1 != null ? e1 : e2,
                                (e1, e2) -> e1 != null ? e1 : e2);

        JavaPairRDD<String, Long> rangeCountRDD =
                idCount.mapToPair(new UniParcCrossRefCountRangeMapper()).reduceByKey(Long::sum);

        Map<String, Long> rangeCountMap = rangeCountRDD.collectAsMap();
        Map<String, Long> sortedByKeyMap = new TreeMap<>(rangeCountMap);
        log.info("Summary of UniParc entries by cross-reference count ranges: [Range] => [Count]");
        long total = 0;
        for (Map.Entry<String, Long> entry : sortedByKeyMap.entrySet()) {
            log.info("[{}] ==> {}", entry.getKey(), entry.getValue());
            total += entry.getValue();
        }
        log.info("Total unique UniParc entry count {}", total);
    }

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name"
                            + "args[1]=spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)");
        }

        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(applicationConfig, args[1])) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();
            UniParcCrossRefRangeCountJob job = new UniParcCrossRefRangeCountJob(jobParameter);
            //            job.countCrossRefByRange();
            //            job.countMostRepeatedTaxonomyIdByUniRefId();
            job.findMostRepeatedTaxonomy();
            log.info("The cross ref range job submitted!");
        } catch (Exception e) {
            throw new IndexDataStoreException(
                    "Unexpected error during counting cross reference", e);
        } finally {
            log.info(
                    "Finished preparing cross reference range and corresponding uniparc entry count.");
            log.info("See the logs for result summary.");
        }
    }
}
