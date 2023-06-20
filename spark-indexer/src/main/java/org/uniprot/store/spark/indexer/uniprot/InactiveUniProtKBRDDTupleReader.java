package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.Collections;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniprot.mapper.InactiveEntryAggregationMapper;
import org.uniprot.store.spark.indexer.uniprot.mapper.InactiveFileToInactiveEntry;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtEntryToSolrDocument;

import com.typesafe.config.Config;

/**
 * This class load a JavaPairRDD with <accession, UniProtDocument> for Inactive UniProt Entries.
 *
 * @author lgonzales
 * @since 2019-12-02
 */
public class InactiveUniProtKBRDDTupleReader {

    private InactiveUniProtKBRDDTupleReader() {}

    /** @return an JavaPairRDD with <accession, UniProtDocument> for Inactive UniProt Entries. */
    public static JavaPairRDD<String, UniProtDocument> load(JobParameter jobParameter) {
        Config config = jobParameter.getApplicationConfig();

        SparkSession spark =
                SparkSession.builder()
                        .config(jobParameter.getSparkContext().getConf())
                        .getOrCreate();
        InactiveEntryAggregationMapper aggregationMapper = new InactiveEntryAggregationMapper();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String inactiveFile = releaseInputDir + config.getString("uniprot.inactive.file.path");
        return spark.read()
                .textFile(inactiveFile)
                .toJavaRDD()
                .mapToPair(new InactiveFileToInactiveEntry())
                .aggregateByKey(null, aggregationMapper, aggregationMapper)
                .mapValues(new UniProtEntryToSolrDocument(Collections.emptyMap()));
    }
}
