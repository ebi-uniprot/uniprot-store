package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniref.converter.DatasetUniRefEntryConverter;
import org.uniprot.store.spark.indexer.uniref.converter.DatasetUniRefEntryLightConverter;

/**
 * Responsible for loading the JavaRDD {@link UniRefEntryLight} for a specific {@link UniRefType}
 *
 * <p>Created 08/07/2020
 *
 * @author Edd
 */
public class UniRefLightRDDTupleReader implements RDDReader<UniRefEntryLight> {

    private final JobParameter jobParameter;
    private final UniRefType uniRefType;
    private final boolean shouldRepartition;

    public UniRefLightRDDTupleReader(
            UniRefType uniRefType, JobParameter jobParameter, boolean shouldRepartition) {
        this.uniRefType = uniRefType;
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    public JavaRDD<UniRefEntryLight> load() {
        JavaRDD<Row> uniRefEntryDataset = loadRawXml().toJavaRDD();
        if (shouldRepartition) {
            uniRefEntryDataset =
                    uniRefEntryDataset.repartition(uniRefEntryDataset.getNumPartitions() * 7);
        }

        return uniRefEntryDataset.map(new DatasetUniRefEntryLightConverter(uniRefType));
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
