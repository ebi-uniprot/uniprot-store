package org.uniprot.store.spark.indexer.uniparc;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter;

import com.typesafe.config.Config;

/**
 * Responsible to Load JavaRDD{UniParcEntry}
 *
 * @author lgonzales
 * @since 2020-02-13
 */
public class UniParcRDDTupleReader implements RDDReader<UniParcEntry> {

    private final JobParameter jobParameter;
    private final boolean shouldRepartition;

    public UniParcRDDTupleReader(JobParameter jobParameter, boolean shouldRepartition) {
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    public JavaRDD<UniParcEntry> load() {
        Config config = jobParameter.getApplicationConfig();
        int repartition = Integer.parseInt(config.getString("uniparc.repartition"));
        Dataset<Row> uniParcEntryDataset = loadRawXml();
        if (shouldRepartition && repartition > 0) {
            uniParcEntryDataset = uniParcEntryDataset.repartition(repartition);
        }

        Encoder<UniParcEntry> entryEncoder =
                (Encoder<UniParcEntry>) Encoders.kryo(UniParcEntry.class);
        return uniParcEntryDataset
                .map(new DatasetUniParcEntryConverter(), entryEncoder)
                .toJavaRDD();
    }

    private Dataset<Row> loadRawXml() {
        Config config = jobParameter.getApplicationConfig();
        SparkSession spark =
                SparkSession.builder()
                        .config(jobParameter.getSparkContext().getConf())
                        .getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String xmlFilePath = releaseInputDir + config.getString("uniparc.xml.file");
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
