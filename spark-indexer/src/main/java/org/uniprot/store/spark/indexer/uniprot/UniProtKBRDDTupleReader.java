package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseMainThreadDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.uniprot.converter.SupportingDataMapHDSFImpl;
import org.uniprot.store.spark.indexer.uniprot.mapper.FlatFileToUniprotEntry;

/**
 * This class load an JavaPairRDD with <accession, UniProtKBEntry>
 *
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniProtKBRDDTupleReader implements PairRDDReader<String, UniProtKBEntry> {

    private final JobParameter jobParameter;
    private final boolean shouldRepartition;

    public UniProtKBRDDTupleReader(JobParameter jobParameter, boolean shouldRepartition) {
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    private static final String SPLITTER = "\n//\n";

    /** @return an JavaPairRDD with <accession, UniProtKBEntry> */
    @Override
    public JavaPairRDD<String, UniProtKBEntry> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        String releaseInputDir =
                getInputReleaseMainThreadDirPath(config, jobParameter.getReleaseName());
        String keywordFile = releaseInputDir + config.getString("keyword.file.path");
        String diseaseFile = releaseInputDir + config.getString("disease.file.path");
        String subcellularLocationFile = releaseInputDir + config.getString("subcell.file.path");

        SupportingDataMapHDSFImpl supportingDataMap =
                new SupportingDataMapHDSFImpl(
                        keywordFile,
                        diseaseFile,
                        subcellularLocationFile,
                        jsc.hadoopConfiguration());

        JavaRDD<String> splittedFileRDD = loadFlatFileToRDD();
        if (shouldRepartition) {
            // in the end when I save the document, it generate 3 times
            // the number of partition, By doing it at the beginning it
            // run the process faster when uses join.
            splittedFileRDD = splittedFileRDD.repartition(splittedFileRDD.getNumPartitions() * 3);
        }
        PairFunction<String, String, UniProtKBEntry> mapper =
                new FlatFileToUniprotEntry(supportingDataMap);
        return splittedFileRDD.map(e -> e + SPLITTER).mapToPair(mapper);
    }

    /** @return Return an RDD with the entry in String format */
    public JavaRDD<String> loadFlatFileToRDD() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("uniprot.flat.file");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        return jsc.textFile(filePath);
    }
}
