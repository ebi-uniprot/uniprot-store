package org.uniprot.store.spark.indexer.uniprot;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.uniprot.converter.SupportingDataMapHDSFImpl;
import org.uniprot.store.spark.indexer.uniprot.mapper.FlatFileToUniprotEntry;

/**
 * This class load an JavaPairRDD with <accession, UniProtKBEntry>
 *
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniProtKBRDDTupleReader {

    private static final String SPLITTER = "\n//\n";

    /** @return an JavaPairRDD with <accession, UniProtKBEntry> */
    public static JavaPairRDD<String, UniProtKBEntry> load(
            JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String keywordFile = applicationConfig.getString("keyword.file.path");
        String diseaseFile = applicationConfig.getString("disease.file.path");
        String subcellularLocationFile = applicationConfig.getString("subcell.file.path");

        SupportingDataMapHDSFImpl supportingDataMap =
                new SupportingDataMapHDSFImpl(
                        keywordFile,
                        diseaseFile,
                        subcellularLocationFile,
                        jsc.hadoopConfiguration());

        PairFunction<String, String, UniProtKBEntry> mapper =
                new FlatFileToUniprotEntry(supportingDataMap);
        JavaRDD<String> splittedFileRDD = loadFlatFileToRDD(jsc, applicationConfig);

        return (JavaPairRDD<String, UniProtKBEntry>)
                splittedFileRDD
                        // in the end when I save the document, it generate 3 times
                        // the number of partition, By doing it at the beginning it
                        // run the process faster.
                        .repartition(splittedFileRDD.getNumPartitions() * 3)
                        .map(e -> e + SPLITTER)
                        .mapToPair(mapper);
    }

    /** @return Return an RDD with the entry in String format */
    public static JavaRDD<String> loadFlatFileToRDD(
            JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String filePath = applicationConfig.getString("uniprot.flat.file");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        return jsc.textFile(filePath);
    }
}
