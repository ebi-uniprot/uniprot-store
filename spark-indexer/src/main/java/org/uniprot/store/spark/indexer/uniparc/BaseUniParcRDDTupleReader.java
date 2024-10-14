package org.uniprot.store.spark.indexer.uniparc;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniparc.converter.BaseUniParcEntryConverter;

import com.typesafe.config.Config;

public abstract class BaseUniParcRDDTupleReader<V> implements RDDReader<V> {
    protected final JobParameter jobParameter;
    protected final boolean shouldRepartition;

    BaseUniParcRDDTupleReader(JobParameter jobParameter, boolean shouldRepartition) {
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    @Override
    public JavaRDD<V> load() {
        Config config = jobParameter.getApplicationConfig();
        int repartition = Integer.parseInt(config.getString("uniparc.repartition"));
        Dataset<Row> uniParcEntryDataset = loadRawXml();
        if (shouldRepartition && repartition > 0) {
            uniParcEntryDataset = uniParcEntryDataset.repartition(repartition);
        }

        Encoder<V> entryEncoder = getEncoder();
        return uniParcEntryDataset.map(getConverter(), entryEncoder).toJavaRDD();
    }

    protected abstract Encoder<V> getEncoder();

    protected abstract MapFunction<Row, V> getConverter();

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
                        .schema(BaseUniParcEntryConverter.getUniParcXMLSchema())
                        .load(xmlFilePath);
        data.printSchema();
        return data;
    }
}
