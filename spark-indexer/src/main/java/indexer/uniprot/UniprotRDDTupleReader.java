package indexer.uniprot;

import indexer.uniprot.converter.SupportingDataMapHDSFImpl;
import indexer.uniprot.mapper.FlatFileToUniprotEntry;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniprot.UniProtEntry;

import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniprotRDDTupleReader {

    private final static String SPLITTER = "\n//\n";

    public static JavaPairRDD<String, UniProtEntry> load(JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String keywordFile = applicationConfig.getString("keyword.file.path");
        String diseaseFile = applicationConfig.getString("disease.file.path");
        String subcellularLocationFile = applicationConfig.getString("subcell.file.path");

        SupportingDataMapHDSFImpl supportingDataMap = new SupportingDataMapHDSFImpl(keywordFile, diseaseFile,
                subcellularLocationFile, jsc.hadoopConfiguration());

        PairFunction<String, String, UniProtEntry> mapper = new FlatFileToUniprotEntry(supportingDataMap);
        JavaRDD<String> splittedFileRDD = loadFlatFileToRDD(jsc, applicationConfig);

        return (JavaPairRDD<String, UniProtEntry>) splittedFileRDD
                .repartition(splittedFileRDD.getNumPartitions() * 2)
                .map(e -> e + SPLITTER)
                .mapToPair(mapper);
    }

    public static JavaRDD<String> loadFlatFileToRDD(JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String filePath = applicationConfig.getString("uniprot.flat.file");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        return jsc.textFile(filePath);
    }
}
