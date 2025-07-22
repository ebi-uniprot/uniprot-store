package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoogleUniProtXMLEntryExtractor;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoogleUniProtXMLToUniProtEntryMapper;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GoogleUniProtKBRDDReader implements PairRDDReader<String, UniProtKBEntry> {
    private final JobParameter jobParameter;

    public GoogleUniProtKBRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, UniProtKBEntry> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("google.protlm.xml.file");
        JavaRDD<String> lines = jsc.textFile(filePath, 1);
        log.info("Total lines in file: {}", lines.count());
        // Step 1: Reconstruct <entry> blocks
        JavaRDD<String> entryXMLs =
                lines.map(String::trim)
                        .filter(line -> !line.isEmpty())
                        .mapPartitions(new GoogleUniProtXMLEntryExtractor());
        // Step 2 - convert xml entry to accession, uniprotkbentry pair
        JavaPairRDD<String, UniProtKBEntry> uniProtKBPairRDD =
                entryXMLs.mapToPair(new GoogleUniProtXMLToUniProtEntryMapper());
        return uniProtKBPairRDD;
    }
}
