package org.uniprot.store.spark.indexer.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.uniprot.store.search.document.Document;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * @author lgonzales
 * @since 2019-11-03
 */
@Slf4j
public class SolrUtils {

    private SolrUtils() {}

    public static void saveSolrInputDocumentRDD(
            JavaPairRDD<String, ? extends Document> docRDD, String savePath) {
        saveSolrInputDocumentRDD(docRDD.values(), savePath);
    }

    public static void saveSolrInputDocumentRDD(
            JavaRDD<? extends Document> docRDD, String savePath) {
        RDD<SolrInputDocument> solrInputDocumentRDD =
                docRDD.map(SolrUtils::convertToSolrInputDocument).rdd();
        log.info("Document count to save on HDFS: {}", solrInputDocumentRDD.count());
        long idCount =
                solrInputDocumentRDD
                        .toJavaRDD()
                        .mapToPair(x -> new Tuple2<>(x.getField("id"), x))
                        .groupByKey()
                        .count();
        log.info("Document ID count to save on HDFS: {}", idCount);
        solrInputDocumentRDD
                .toJavaRDD()
                .mapToPair(x -> new Tuple2<>(x.getField("accession").getFirstValue(), x))
                .groupByKey()
                .mapValues(f -> StreamSupport.stream(f.spliterator(), false).count())
                .filter(x -> x._2 > 1000)
                .take(200)
                .forEach(x -> log.info("accession:{}, count:{}", x._1, x._2));

        //        solrInputDocumentRDD.saveAsObjectFile(savePath);
    }

//    public static void main(String[] args) {
//        List<Integer> thing = asList(1, 2, 3);
//
//        System.out.println(StreamSupport.stream(thing.spliterator(), false).count());
//    }

    public static SolrInputDocument convertToSolrInputDocument(Document doc) {
        DocumentObjectBinder binder = new DocumentObjectBinder();
        return binder.toSolrInputDocument(doc);
    }

    public static void commit(String collection, String zkHost) {
        log.info("Committing the data for collection " + collection);
        try (CloudSolrClient client =
                new CloudSolrClient.Builder(singletonList(zkHost), Optional.empty()).build()) {
            client.commit(collection, true, true);
        } catch (Exception e) {
            log.error("Error committing the data for collection" + collection, e);
        }
        log.info("Completed commit the data for collection " + collection);
    }
}
