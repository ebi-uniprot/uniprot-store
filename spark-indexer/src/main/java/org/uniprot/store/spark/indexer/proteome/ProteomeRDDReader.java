package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.proteome.converter.ProteomEntryToPair;
import org.uniprot.store.spark.indexer.proteome.converter.ProteomeXMLSchema;
import org.uniprot.store.spark.indexer.proteome.converter.RowProteomeEntryConverter;

import java.util.ResourceBundle;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

/**
 * @author sahmad
 * @created 21/08/2020
 */
public class ProteomeRDDReader implements PairRDDReader<String, ProteomeEntry> {

    private final JobParameter jobParameter;
    private final boolean shouldRepartition;

    public ProteomeRDDReader(JobParameter jobParameter, boolean shouldRepartition){
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    @Override
    public JavaPairRDD<String, ProteomeEntry> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        int repartition = Integer.parseInt(config.getString("proteome.repartition"));

        JavaRDD<Row> proteomeEntryDataset = loadRawXml().toJavaRDD();
        if (this.shouldRepartition && repartition > 0) {
            proteomeEntryDataset = proteomeEntryDataset.repartition(repartition);
        }

        return proteomeEntryDataset.map(new RowProteomeEntryConverter()).mapToPair(new ProteomEntryToPair());
    }

    private Dataset<Row> loadRawXml() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        SparkSession spark =
                SparkSession.builder()
                        .config(jobParameter.getSparkContext().getConf())
                        .getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String xmlFilePath = releaseInputDir + config.getString("proteome.xml.file");
        Dataset<Row> data =
                spark.read()
                        .format("com.databricks.spark.xml")
                        .option("rowTag", "proteome")
                        .schema(ProteomeXMLSchema.geProteomeXMLSchema())
                        .load(xmlFilePath);
        data.printSchema();
        return data;
    }
}
