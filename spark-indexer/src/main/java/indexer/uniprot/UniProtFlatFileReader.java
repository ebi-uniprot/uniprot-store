package indexer.uniprot;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.uniprot.UniProtEntry;

/**
 * @author lgonzales
 * @since 2019-10-03
 */
public class UniProtFlatFileReader {

    private final SparkConf sparkConf;
    private final String flatFilePath;
    private final static String SPLITTER = "\n//\n";

    public UniProtFlatFileReader(SparkConf sparkConf, String flatFilePath) {
        this.sparkConf = sparkConf;
        this.flatFilePath = flatFilePath;
    }


    public Dataset<String> readRawFlatFile() {
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        Dataset<String> data = spark.read()
                .option("lineSep", SPLITTER)
                .textFile(flatFilePath);
        return data;
    }

    public Dataset<UniProtEntry> read() {
        return readRawFlatFile().map(new DatasetUniprotEntryConverter(), Encoders.kryo(UniProtEntry.class));
    }
}
