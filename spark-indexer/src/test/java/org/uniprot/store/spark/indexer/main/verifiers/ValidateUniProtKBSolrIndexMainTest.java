package org.uniprot.store.spark.indexer.main.verifiers;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

class ValidateUniProtKBSolrIndexMainTest {

    @Test
    void errorWithInvalidArguments() throws Exception {
        assertThrows(
                IllegalArgumentException.class,
                () -> ValidateUniProtKBSolrIndexMain.main(new String[2]));
    }

    @Test
    void canRunValidation() throws Exception {
        ValidateUniProtKBSolrIndexMain validator =
                Mockito.spy(new ValidateUniProtKBSolrIndexMain());
        Mockito.doReturn(1L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.REVIEWED_QUERY));
        Mockito.doReturn(0L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.UNREVIEWED_QUERY));
        Mockito.doReturn(0L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.ISOFORM_QUERY));
        assertDoesNotThrow(() -> validator.runValidation("2020_02"));
    }

    @Test
    void canRunInvalidReviewedValidation() throws Exception {
        ValidateUniProtKBSolrIndexMain validator =
                Mockito.spy(new ValidateUniProtKBSolrIndexMain());
        Mockito.doReturn(5L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.REVIEWED_QUERY));
        Mockito.doReturn(0L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.UNREVIEWED_QUERY));
        Mockito.doReturn(0L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.ISOFORM_QUERY));
        SparkIndexException error =
                assertThrows(SparkIndexException.class, () -> validator.runValidation("2020_02"));
        assertEquals("reviewed does not match. solr count: 5 RDD count 1", error.getMessage());
    }

    @Test
    void canRunInvalidUnreviewedValidation() throws Exception {
        ValidateUniProtKBSolrIndexMain validator =
                Mockito.spy(new ValidateUniProtKBSolrIndexMain());
        Mockito.doReturn(1L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.REVIEWED_QUERY));
        Mockito.doReturn(5L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.UNREVIEWED_QUERY));
        Mockito.doReturn(0L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.ISOFORM_QUERY));
        SparkIndexException error =
                assertThrows(SparkIndexException.class, () -> validator.runValidation("2020_02"));
        assertEquals("unReviewed does not match. solr count: 5 RDD count 0", error.getMessage());
    }

    @Test
    void canRunInvalidIsoformValidation() throws Exception {
        ValidateUniProtKBSolrIndexMain validator =
                Mockito.spy(new ValidateUniProtKBSolrIndexMain());
        Mockito.doReturn(1L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.REVIEWED_QUERY));
        Mockito.doReturn(0L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.UNREVIEWED_QUERY));
        Mockito.doReturn(5L)
                .when(validator)
                .getSolrCount(
                        Mockito.any(), Mockito.eq(ValidateUniProtKBSolrIndexMain.ISOFORM_QUERY));
        SparkIndexException error =
                assertThrows(SparkIndexException.class, () -> validator.runValidation("2020_02"));
        assertEquals(
                "reviewed isoform does not match. solr count: 5 RDD count 0", error.getMessage());
    }
}
