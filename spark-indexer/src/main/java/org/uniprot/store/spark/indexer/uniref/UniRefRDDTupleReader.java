package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniref.converter.DatasetUniRefEntryConverter;

/**
 * Responsible to Load JavaRDD{UniRefEntry} for a specific UniRefType
 *
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniRefRDDTupleReader implements RDDReader<UniRefEntry> {

    private final JobParameter jobParameter;
    private final UniRefType uniRefType;
    private final boolean shouldRepartition;

    public UniRefRDDTupleReader(
            UniRefType uniRefType, JobParameter jobParameter, boolean shouldRepartition) {
        this.uniRefType = uniRefType;
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    public JavaRDD<UniRefEntry> load() {
        JavaRDD<Row> uniRefEntryDataset = loadRawXml().toJavaRDD();
        if (shouldRepartition) {
            uniRefEntryDataset =
                    uniRefEntryDataset.repartition(uniRefEntryDataset.getNumPartitions() * 7);
        }

        return uniRefEntryDataset.map(new DatasetUniRefEntryConverter(uniRefType));
    }

    private Dataset<Row> loadRawXml() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String propertyPrefix = uniRefType.toString().toLowerCase();
        String xmlFilePath = releaseInputDir + config.getString(propertyPrefix + ".xml.file");

        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
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
