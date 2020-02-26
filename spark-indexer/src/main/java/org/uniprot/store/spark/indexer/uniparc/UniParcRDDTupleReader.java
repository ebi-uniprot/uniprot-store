package org.uniprot.store.spark.indexer.uniparc;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter;

import scala.Serializable;

/**
 * Responsible to Load JavaRDD{UniParcEntry}
 *
 * @author lgonzales
 * @since 2020-02-13
 */
public class UniParcRDDTupleReader implements Serializable {
    private static final long serialVersionUID = 9032437078231492159L;

    public static JavaRDD<UniParcEntry> load(
            SparkConf sparkConf, ResourceBundle applicationConfig) {

        String xmlFilePath = applicationConfig.getString("uniparc.xml.file");
        Integer repartition = new Integer(applicationConfig.getString("uniparc.repartition"));
        Dataset<Row> uniParcEntryDataset = loadRawXml(sparkConf, xmlFilePath);
        if (repartition > 0) {
            uniParcEntryDataset = uniParcEntryDataset.repartition(repartition);
        }

        Encoder<UniParcEntry> entryEncoder =
                (Encoder<UniParcEntry>) Encoders.kryo(UniParcEntry.class);
        return uniParcEntryDataset
                .map(new DatasetUniParcEntryConverter(), entryEncoder)
                .toJavaRDD();
    }

    private static Dataset<Row> loadRawXml(SparkConf sparkConf, String xmlFilePath) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> data =
                spark.read()
                        .format("com.databricks.spark.xml")
                        .option("rowTag", "entry")
                        .schema(DatasetUniParcEntryConverter.getUniParcXMLSchema())
                        .load(xmlFilePath);
        data.printSchema();
        return data;
    }
}
