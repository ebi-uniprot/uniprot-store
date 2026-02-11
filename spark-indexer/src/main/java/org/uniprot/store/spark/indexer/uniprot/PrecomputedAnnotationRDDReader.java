package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;
import static org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader.SPLITTER;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.NotNull;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniprot.converter.SupportingDataMapHDSFImpl;
import org.uniprot.store.spark.indexer.uniprot.mapper.PrecomputedAnnotationFlatFileToUniProtKBEntry;

import com.typesafe.config.Config;

public class PrecomputedAnnotationRDDReader implements RDDReader<UniProtKBEntry> {
    private final JobParameter jobParameter;

    public PrecomputedAnnotationRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaRDD<UniProtKBEntry> load() {
        Config config = this.jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String fullFilePath = releaseInputDir + config.getString("precomputed.annotation.file");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        JavaRDD<String> entryStrRDD = jsc.textFile(fullFilePath);

        SupportingDataMapHDSFImpl supportingDataMap =
                getSupportingDataMap(releaseInputDir, config, jsc);

        PrecomputedAnnotationFlatFileToUniProtKBEntry flatFileToUniProtKBEntry =
                new PrecomputedAnnotationFlatFileToUniProtKBEntry(supportingDataMap);
        JavaRDD<UniProtKBEntry> uniProtKBRDD =
                entryStrRDD.map(e -> e + SPLITTER).map(flatFileToUniProtKBEntry);
        return uniProtKBRDD;
    }

    private static @NotNull SupportingDataMapHDSFImpl getSupportingDataMap(
            String releaseInputDir, Config config, JavaSparkContext jsc) {
        String keywordFile = releaseInputDir + config.getString("keyword.file.path");
        String diseaseFile = releaseInputDir + config.getString("disease.file.path");
        String subcellularLocationFile = releaseInputDir + config.getString("subcell.file.path");
        SupportingDataMapHDSFImpl supportingDataMap =
                new SupportingDataMapHDSFImpl(
                        keywordFile,
                        diseaseFile,
                        subcellularLocationFile,
                        jsc.hadoopConfiguration());
        return supportingDataMap;
    }
}
