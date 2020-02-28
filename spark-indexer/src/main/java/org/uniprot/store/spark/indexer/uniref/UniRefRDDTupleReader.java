package org.uniprot.store.spark.indexer.uniref;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.uniref.converter.DatasetUniRefEntryConverter;

import scala.Serializable;

/**
 * Responsible to Load JavaRDD{UniRefEntry} for a specific UniRefType
 *
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniRefRDDTupleReader implements Serializable {
    private static final long serialVersionUID = -4292235298850285242L;

    public static JavaRDD<UniRefEntry> load(
            UniRefType uniRefType, SparkConf sparkConf, ResourceBundle applicationConfig) {
        String propertyPrefix = uniRefType.toString().toLowerCase();
        String xmlFilePath = applicationConfig.getString(propertyPrefix + ".xml.file");
        Integer repartition =
                new Integer(applicationConfig.getString(propertyPrefix + ".repartition"));
        Dataset<Row> uniRefEntryDataset = loadRawXml(sparkConf, xmlFilePath);
        if (repartition > 0) {
            uniRefEntryDataset = uniRefEntryDataset.repartition(repartition);
        }
        Encoder<UniRefEntry> entryEncoder = (Encoder<UniRefEntry>) Encoders.kryo(UniRefEntry.class);
        return uniRefEntryDataset
                .map(new DatasetUniRefEntryConverter(uniRefType), entryEncoder)
                .toJavaRDD();
    }

    private static Dataset<Row> loadRawXml(SparkConf sparkConf, String xmlFilePath) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> data =
                spark.read()
                        .format("com.databricks.spark.xml")
                        .option("rowTag", "entry")
                        .schema(DatasetUniRefEntryConverter.getUniRefXMLSchema())
                        .load(xmlFilePath);
        data.printSchema();
        return data;
    }
}
