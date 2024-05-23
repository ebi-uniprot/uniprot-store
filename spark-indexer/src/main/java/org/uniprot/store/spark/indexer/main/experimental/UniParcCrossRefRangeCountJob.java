package org.uniprot.store.spark.indexer.main.experimental;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniparc.UniParcRDDTupleReader;
import scala.Tuple2;

import java.util.Map;
import java.util.TreeMap;

@Slf4j
public class UniParcCrossRefRangeCountJob {

    private final JobParameter parameter;

    public UniParcCrossRefRangeCountJob(JobParameter parameter) {
        this.parameter = parameter;
    }

    //cross ref range -> uniparc entry count. e.g. 1000-2000 --> 200
    public void countCrossRefByRange(){
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniParcRDD = reader.load();

        log.info("Total UniParc Entry count in input file {}", uniParcRDD.count());

        JavaPairRDD<String, Integer> idCount = uniParcRDD.mapToPair(entry -> new Tuple2<>(entry.getUniParcId().getValue(), entry.getUniParcCrossReferences().size()))
                .aggregateByKey(null, (e1, e2) -> e1 != null ? e1 : e2, (e1, e2) -> e1 != null ? e1 : e2);

        JavaPairRDD<String, Long> rangeCountRDD = idCount.mapToPair(new UniParcCrossRefCountRangeMapper()).reduceByKey(Long::sum);

        Map<String, Long> rangeCountMap = rangeCountRDD.collectAsMap();
        Map<String, Long> sortedByKeyMap = new TreeMap<>(rangeCountMap);
        log.info("Summary of UniParc entries by cross-reference count ranges: [Range] => [Count]");
        long total = 0;
        for(Map.Entry<String, Long> entry:sortedByKeyMap.entrySet()){
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
            job.countCrossRefByRange();
            log.info("The cross ref range job submitted!");
        } catch (Exception e) {
            throw new IndexDataStoreException("Unexpected error during counting cross reference", e);
        } finally {
            log.info("Finished preparing cross reference range and corresponding uniparc entry count.");
            log.info("See the logs for result summary.");
        }
    }
}
