package org.uniprot.store.spark.indexer.validator.impl;

import static java.util.Collections.singletonList;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.flatfile.parser.impl.cc.CcLineTransformer;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil;
import org.uniprot.store.spark.indexer.validator.SolrIndexValidator;

import com.typesafe.config.Config;

@Slf4j
public class UniProtKBSolrIndexValidator implements SolrIndexValidator {

    static final String REVIEWED_QUERY = "reviewed:true AND active:true AND is_isoform:false";
    static final String ISOFORM_QUERY = "reviewed:true AND active:true AND is_isoform:true";
    static final String UNREVIEWED_QUERY = "reviewed:false AND active:true";
    private final JobParameter jobParameter;

    public UniProtKBSolrIndexValidator(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public void runValidation() {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        String zkHost = applicationConfig.getString("solr.zkhost");
        try {
            long reviewed = 0;
            long reviewedIsoform = 0;
            long unReviewed = 0;
            try (CloudSolrClient client =
                    new CloudSolrClient.Builder(singletonList(zkHost), Optional.empty()).build()) {
                reviewed = getSolrCount(client, REVIEWED_QUERY);
                log.info("reviewed Solr COUNT: " + reviewed);

                reviewedIsoform = getSolrCount(client, ISOFORM_QUERY);
                log.info("reviewed isoform Solr COUNT: " + reviewedIsoform);

                unReviewed = getSolrCount(client, UNREVIEWED_QUERY);
                log.info("unReviewed Solr COUNT: " + unReviewed);
            } catch (Exception e) {
                log.error("Error executing uniprotkb Validation: ", e);
            }

            UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(jobParameter, false);
            JavaRDD<String> flatFileRDD = reader.loadFlatFileToRDD();

            long rddReviewed = getRDDReviewedCount(flatFileRDD);
            long rddReviewedIsoform = getRDDReviewedIsoformCount(flatFileRDD);
            long rddUnReviewed = getRDDUnreviewedCount(flatFileRDD);

            JavaRDD<SolrInputDocument> outputDocuments = getOutputUniProtKBDocuments();

            long outputReviewed = getOutputReviewedCount(outputDocuments);
            long outputReviewedIsoform = getOutputReviewedIsoformCount(outputDocuments);
            long outputUnReviewed = getOutputUnreviewedCount(outputDocuments);

            log.info(
                    "reviewed isoform DocumentOutput COUNT: "
                            + outputReviewedIsoform
                            + ", RDD COUNT: "
                            + rddReviewedIsoform
                            + ", Solr COUNT: "
                            + reviewedIsoform);

            log.info(
                    "reviewed DocumentOutput COUNT: "
                            + outputReviewed
                            + ", RDD COUNT: "
                            + rddReviewed
                            + ", Solr COUNT: "
                            + reviewed);

            log.info(
                    "unReviewed DocumentOutput COUNT: "
                            + outputUnReviewed
                            + ", RDD COUNT: "
                            + rddUnReviewed
                            + ", Solr COUNT: "
                            + unReviewed);

            if (reviewed != rddReviewed || reviewed != outputReviewed) {
                throw new SparkIndexException(
                        "reviewed does not match. "
                                + "DocumentOutput COUNT: "
                                + outputReviewed
                                + ", RDD COUNT: "
                                + rddReviewed
                                + ", Solr COUNT: "
                                + reviewed);
            }
            if (reviewedIsoform != rddReviewedIsoform || reviewedIsoform != outputReviewedIsoform) {
                throw new SparkIndexException(
                        "reviewed isoform does not match. "
                                + "DocumentOutput COUNT: "
                                + outputReviewedIsoform
                                + ", RDD COUNT: "
                                + rddReviewedIsoform
                                + ", Solr COUNT: "
                                + reviewedIsoform);
            }
            if (unReviewed != rddUnReviewed || unReviewed != outputUnReviewed) {
                throw new SparkIndexException(
                        "unReviewed does not match. "
                                + "DocumentOutput COUNT: "
                                + outputUnReviewed
                                + ", RDD COUNT: "
                                + rddUnReviewed
                                + ", Solr COUNT: "
                                + unReviewed);
            }
        } catch (SparkIndexException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexHPSDocumentsException("Unexpected error during uniprot validation.", e);
        } finally {
            log.info("Finished check for uniprot");
        }
    }

    JavaRDD<SolrInputDocument> getOutputUniProtKBDocuments() {
        String hpsOutputFilePath =
                getCollectionOutputReleaseDirPath(
                        jobParameter.getApplicationConfig(),
                        jobParameter.getReleaseName(),
                        SolrCollection.uniprot);
        return jobParameter
                .getSparkContext()
                .objectFile(hpsOutputFilePath)
                .map(obj -> (SolrInputDocument) obj);
    }

    private long getOutputUnreviewedCount(JavaRDD<SolrInputDocument> outputDocuments) {
        return outputDocuments
                .map(doc -> doc.getFieldValue("reviewed"))
                .filter(Objects::nonNull)
                .filter(reviewed -> Boolean.valueOf(reviewed.toString()).equals(Boolean.FALSE))
                .count();
    }

    private long getOutputReviewedIsoformCount(JavaRDD<SolrInputDocument> outputDocuments) {
        return outputDocuments.filter(UniProtKBSolrIndexValidator::filterIsoform).count();
    }

    private static boolean filterIsoform(SolrInputDocument doc) {
        boolean isIsoform = false;
        Object reviewed = doc.getFieldValue("reviewed");
        if (reviewed != null && Boolean.valueOf(reviewed.toString()).equals(Boolean.TRUE)) {
            String accession = doc.getFieldValue("accession_id").toString();
            Object canonical = doc.getFieldValue("canonical_acc");
            if (accession.contains("-") && canonical != null) {
                isIsoform = true;
            }
        }
        return isIsoform;
    }

    private long getOutputReviewedCount(JavaRDD<SolrInputDocument> outputDocuments) {
        return outputDocuments.filter(UniProtKBSolrIndexValidator::filterReviewed).count();
    }

    private static boolean filterReviewed(SolrInputDocument doc) {
        boolean isReviewed = false;
        Object reviewed = doc.getFieldValue("reviewed");
        if (reviewed != null && Boolean.valueOf(reviewed.toString()).equals(Boolean.TRUE)) {
            String accession = doc.getFieldValue("accession_id").toString();
            if (!accession.contains("-")) {
                isReviewed = true;
            }
        }
        return isReviewed;
    }

    long getRDDUnreviewedCount(JavaRDD<String> flatFileRDD) {
        return flatFileRDD
                .map(entryStr -> entryStr.split("\n")[0])
                .filter(entryStr -> entryStr.contains("Unreviewed;"))
                .count();
    }

    long getRDDReviewedIsoformCount(JavaRDD<String> flatFileRDD) {
        return flatFileRDD
                .filter(
                        entryStr -> {
                            String[] linesArray = entryStr.split("\n");
                            String idLine = linesArray[0];
                            String acc = linesArray[1].split(";")[0];
                            return idLine.contains("Reviewed;") && acc.contains("-");
                        })
                .filter(UniProtKBSolrIndexValidator::filterCanonicalIsoform)
                .count();
    }

    long getRDDReviewedCount(JavaRDD<String> flatFileRDD) {
        return flatFileRDD
                .filter(
                        entryStr -> {
                            String[] linesArray = entryStr.split("\n");
                            String idLine = linesArray[0];
                            String acc = linesArray[1].split(";")[0];
                            return idLine.contains("Reviewed;") && !acc.contains("-");
                        })
                .count();
    }

    long getSolrCount(CloudSolrClient client, String query)
            throws SolrServerException, IOException {
        ModifiableSolrParams queryParams = new ModifiableSolrParams();
        queryParams.set("q", query);
        queryParams.set("fl", "accession_id");
        QueryResponse result = client.query(SolrCollection.uniprot.name(), queryParams);
        return result.getResults().getNumFound();
    }

    static boolean filterCanonicalIsoform(String entryStr) {
        String[] entryLineArray = entryStr.split("\n");
        String ccLines =
                Arrays.stream(entryLineArray)
                        .filter(line -> line.startsWith("CC       ") || line.startsWith("CC   -!"))
                        .collect(Collectors.joining("\n"));
        String accession = entryLineArray[1].split(" {3}")[1].split(";")[0].strip();
        List<Comment> comments = new ArrayList<>();
        if (!ccLines.isEmpty()) {
            final CcLineTransformer transformer = new CcLineTransformer();
            comments = transformer.transformNoHeader(ccLines);
        }
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(accession, accession, UniProtKBEntryType.SWISSPROT)
                        .commentsSet(comments)
                        .build();
        return !UniProtEntryConverterUtil.isCanonicalIsoform(entry);
    }
}
