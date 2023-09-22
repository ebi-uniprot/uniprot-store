package org.uniprot.store.spark.indexer.genecentric;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.genecentric.mapper.GeneCentricJoin;
import org.uniprot.store.spark.indexer.genecentric.mapper.GeneCentricToDocument;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
@Slf4j
public class GeneCentricDocumentsToHPSWriter implements DocumentsToHPSWriter {

    private final JobParameter parameter;
    private final Config config;
    private final String releaseName;

    public GeneCentricDocumentsToHPSWriter(JobParameter parameter) {
        this.parameter = parameter;
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
    }

    @Override
    public void writeIndexDocumentsToHPS() {
        GeneCentricCanonicalRDDReader canonicalReader =
                new GeneCentricCanonicalRDDReader(parameter);
        JavaPairRDD<String, GeneCentricEntry> canonicalRDD = canonicalReader.load();

        GeneCentricRelatedRDDReader relatedReader = new GeneCentricRelatedRDDReader(parameter);
        JavaPairRDD<String, Iterable<GeneCentricEntry>> relatedRDD =
                relatedReader.load().groupByKey();

        JavaRDD<GeneCentricDocument> geneCentricDocumentRDD =
                canonicalRDD
                        .leftOuterJoin(relatedRDD)
                        .mapValues(new GeneCentricJoin())
                        .mapValues(new GeneCentricToDocument())
                        .values();

        saveToHPS(geneCentricDocumentRDD);

        log.info("Completed Gene Centric prepare Solr index");
    }

    void saveToHPS(JavaRDD<GeneCentricDocument> geneCentricDocumentRDD) {
        String hpsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.genecentric);
        SolrUtils.saveSolrInputDocumentRDD(geneCentricDocumentRDD, hpsPath);
    }
}
