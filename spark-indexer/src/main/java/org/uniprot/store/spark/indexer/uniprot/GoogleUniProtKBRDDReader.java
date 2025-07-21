package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoogleUniProtXMLEntryExtractor;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoogleUniProtXMLToUniProtEntryMapper;

import com.typesafe.config.Config;

public class GoogleUniProtKBRDDReader implements RDDReader<UniProtKBEntry> {
    private final JobParameter jobParameter;

    public GoogleUniProtKBRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaRDD<UniProtKBEntry> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("google.protlm.xml.file");
        JavaRDD<String> lines = jsc.textFile(filePath, 1);
        // Step 1: Reconstruct <entry> blocks
        JavaRDD<String> entryXMLs =
                lines.map(String::trim)
                        .filter(line -> !line.isEmpty())
                        .mapPartitions(new GoogleUniProtXMLEntryExtractor());
        // Step 2 - convert xml entry to uniprotkbentry
        JavaRDD<UniProtKBEntry> uniProtKBRDD =
                entryXMLs.map(new GoogleUniProtXMLToUniProtEntryMapper());
        return uniProtKBRDD;
    }
}
