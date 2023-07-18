package org.uniprot.store.spark.indexer.main.experimental;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniparc.UniParcRDDTupleReader;

import com.typesafe.config.Config;

public class ListBigUniParcs {

    public static void main(String[] args) {
        if (args == null || args.length != 3) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name (for example: 2020_01)"
                            + "args[1]=spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)"
                            + "args[2]=output file path");
        }
        String releaseName = args[0];
        String sparkMaster = args[1];
        String outputFilePath = args[2];
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(applicationConfig, sparkMaster)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(releaseName)
                            .sparkContext(sparkContext)
                            .build();
            UniParcRDDTupleReader reader = new UniParcRDDTupleReader(jobParameter, true);
            JavaRDD<UniParcEntry> uniParcRDD = reader.load();
            uniParcRDD.filter(entry -> entry.getUniParcCrossReferences().size() >= 10000)
                    .map(entry -> entry.getUniParcId().getValue() + "\t" + entry.getUniParcCrossReferences().size())
                    .repartition(150)
                    .saveAsTextFile(outputFilePath);
        }
    }
}
