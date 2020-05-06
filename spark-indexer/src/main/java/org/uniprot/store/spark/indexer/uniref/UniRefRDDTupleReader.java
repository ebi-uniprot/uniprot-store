package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
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
            UniRefType uniRefType, JobParameter jobParameter, boolean shouldRepartition) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String propertyPrefix = uniRefType.toString().toLowerCase();
        String xmlFilePath = releaseInputDir + config.getString(propertyPrefix + ".xml.file");
        JavaRDD<Row> uniRefEntryDataset = loadRawXml(jsc.getConf(), xmlFilePath).toJavaRDD();

        if (shouldRepartition) {
            uniRefEntryDataset =
                    uniRefEntryDataset.repartition(uniRefEntryDataset.getNumPartitions() * 7);
        }

        return uniRefEntryDataset.map(new DatasetUniRefEntryConverter(uniRefType));
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
