package indexer.uniref;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.*;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;

import scala.Serializable;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniRefRDDTupleReader implements Serializable {
    private static final long serialVersionUID = -4292235298850285242L;

    private static Dataset<Row> loadRawXml(SparkConf sparkConf, String xmlFilePath) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> data =
                spark.read()
                        .format("com.databricks.spark.xml")
                        .option("rowTag", "entry")
                        .schema(DatasetUnirefEntryConverter.getUniRefXMLSchema())
                        .load(xmlFilePath);
        data.printSchema();
        return data;
    }

    public static JavaPairRDD<String, MappedUniRef> load50(
            SparkConf sparkConf, ResourceBundle applicationConfig) {
        String xmlFilePath = applicationConfig.getString("uniref.50.xml.file");
        Integer repartition = new Integer(applicationConfig.getString("uniref.50.repartition"));
        return getMappedUniRef(sparkConf, xmlFilePath, UniRefType.UniRef50, repartition);
    }

    public static JavaPairRDD<String, MappedUniRef> load90(
            SparkConf sparkConf, ResourceBundle applicationConfig) {
        String xmlFilePath = applicationConfig.getString("uniref.90.xml.file");
        Integer repartition = new Integer(applicationConfig.getString("uniref.90.repartition"));
        return getMappedUniRef(sparkConf, xmlFilePath, UniRefType.UniRef90, repartition);
    }

    public static JavaPairRDD<String, MappedUniRef> load100(
            SparkConf sparkConf, ResourceBundle applicationConfig) {
        String xmlFilePath = applicationConfig.getString("uniref.100.xml.file");
        Integer repartition = new Integer(applicationConfig.getString("uniref.100.repartition"));
        return getMappedUniRef(sparkConf, xmlFilePath, UniRefType.UniRef100, repartition);
    }

    private static JavaPairRDD<String, MappedUniRef> getMappedUniRef(
            SparkConf sparkConf, String xmlFilePath, UniRefType uniRefType, Integer repartition) {
        Dataset<Row> uniRefEntryDataset = loadRawXml(sparkConf, xmlFilePath);
        if (repartition > 0) {
            uniRefEntryDataset = uniRefEntryDataset.repartition(repartition);
        }

        Encoder<UniRefEntry> entryEncoder = (Encoder<UniRefEntry>) Encoders.kryo(UniRefEntry.class);
        return (JavaPairRDD<String, MappedUniRef>)
                uniRefEntryDataset
                        .map(new DatasetUnirefEntryConverter(uniRefType), entryEncoder)
                        .toJavaRDD()
                        .flatMapToPair(new UniRefEntryRDDTupleMapper());
    }
}
