package org.uniprot.store.spark.indexer.uniparc;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
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

    public static JavaRDD<UniParcEntry> load(JobParameter jobParameter, boolean shouldRepartition) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String xmlFilePath = releaseInputDir + config.getString("uniparc.xml.file");
        int repartition = Integer.parseInt(config.getString("uniparc.repartition"));
        Dataset<Row> uniParcEntryDataset = loadRawXml(jsc.getConf(), xmlFilePath);
        if (shouldRepartition && repartition > 0) {
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
