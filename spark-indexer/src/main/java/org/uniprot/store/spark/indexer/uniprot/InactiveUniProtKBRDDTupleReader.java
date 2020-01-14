package org.uniprot.store.spark.indexer.uniprot;

import java.util.Collections;
import java.util.ResourceBundle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.uniprot.mapper.InactiveEntryAggregationMapper;
import org.uniprot.store.spark.indexer.uniprot.mapper.InactiveFileToInactiveEntry;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtEntryToSolrDocument;

/**
 * This class load a JavaPairRDD with <accession, UniProtDocument> for Inactive UniProt Entries.
 *
 * @author lgonzales
 * @since 2019-12-02
 */
public class InactiveUniProtKBRDDTupleReader {

    /** @return an JavaPairRDD with <accession, UniProtDocument> for Inactive UniProt Entries. */
    public static JavaPairRDD<String, UniProtDocument> load(
            SparkConf sparkConf, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        InactiveEntryAggregationMapper aggregationMapper = new InactiveEntryAggregationMapper();
        return (JavaPairRDD<String, UniProtDocument>)
                spark.read()
                        .textFile(applicationConfig.getString("uniprot.inactive.file.path"))
                        .toJavaRDD()
                        .mapToPair(new InactiveFileToInactiveEntry())
                        .aggregateByKey(null, aggregationMapper, aggregationMapper)
                        .mapValues(new UniProtEntryToSolrDocument(Collections.emptyMap()));
    }
}
