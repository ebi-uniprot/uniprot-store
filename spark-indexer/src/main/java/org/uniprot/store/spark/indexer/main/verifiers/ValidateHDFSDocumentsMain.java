package org.uniprot.store.spark.indexer.main.verifiers;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getSolrCollection;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * This class is used to print first 200 solr document saved in HDFS. it is helpful during the
 * development phase, because you can visualize what is being saved in HDFS.
 *
 * @author lgonzales
 * @since 2019-12-12
 */
@Slf4j
public class ValidateHDFSDocumentsMain {

    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);


        SolrCollection collection = getSolrCollection(args[1]).get(0);
        String hdfsFilePath =
                getCollectionOutputReleaseDirPath(applicationConfig, args[0], collection);
        log.info("Output Documents Path: {}", hdfsFilePath);
        JavaRDD<SolrInputDocument> solrInputDocumentRDD =
                sparkContext.objectFile(hdfsFilePath).map(obj -> (SolrInputDocument) obj);

        log.info("Total Documents Entries Count: {}", solrInputDocumentRDD.count());
        if(args.length > 2) {
            String idField = args[2];
            long distinctIds = solrInputDocumentRDD
                    .map(doc -> doc.get(idField).toString()).distinct().count();
            log.info("Total distinct Documents Count: {}", distinctIds);
        }
        sparkContext.close();
    }
}
