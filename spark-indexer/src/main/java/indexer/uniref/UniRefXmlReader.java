package indexer.uniref;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.uniref.UniRefEntry;

/**
 * @author jluo
 * @date: 2 Jul 2019
 */

public class UniRefXmlReader {
    private final SparkConf sparkConf;
    private final String xmlFilePath;

    public UniRefXmlReader(SparkConf sparkConf, String xmlFilePath) {
        this.sparkConf = sparkConf;
        this.xmlFilePath = xmlFilePath;
    }


    public Dataset<Row> readRawXml() {
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        Dataset<Row> data = spark.read().format("com.databricks.spark.xml")
                .option("rowTag", "entry")
                .load(xmlFilePath);
        return data;
    }

    public Dataset<UniRefEntry> read() {
        return readRawXml().map(new DatasetUnirefEntryConverter(), Encoders.kryo(UniRefEntry.class));
    }
}

