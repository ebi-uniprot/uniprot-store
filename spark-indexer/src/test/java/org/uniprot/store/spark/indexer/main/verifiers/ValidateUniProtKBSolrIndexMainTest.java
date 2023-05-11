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

    @Test
    void canFilterOutCanonicalIsoforms() {
        String entryStr =
                "ID   NSMF_RAT_1                Reviewed;         532 AA.\n"
                        + "AC   Q9EPI6-1; Q5PPF6; Q7TSC6; Q7TSC8; Q9EPI4; Q9EPI5;\n"
                        + "CC   -!- ALTERNATIVE PRODUCTS:\n"
                        + "CC       Event=Alternative splicing; Named isoforms=5;\n"
                        + "CC         Comment=Additional isoforms seem to exist.;\n"
                        + "CC       Name=1;\n"
                        + "CC         IsoId=Q9EPI6-1; Sequence=Displayed;\n"
                        + "CC       Name=2;\n"
                        + "CC         IsoId=Q9EPI6-2; Sequence=VSP_014771;\n"
                        + "CC       Name=3;\n"
                        + "CC         IsoId=Q9EPI6-3; Sequence=VSP_014770;\n"
                        + "CC         Note=No experimental confirmation available.;\n"
                        + "CC       Name=4;\n"
                        + "CC         IsoId=Q9EPI6-4; Sequence=VSP_014771, VSP_014772;\n"
                        + "CC         Note=No experimental confirmation available.;\n"
                        + "CC       Name=5;\n"
                        + "CC         IsoId=Q9EPI6-5; Sequence=VSP_014773, VSP_014774;\n"
                        + "CC         Note=No experimental confirmation available.;";
        boolean result = ValidateUniProtKBSolrIndexMain.filterCanonicalIsoform(entryStr);
        assertFalse(result);
    }

    @Test
    void canAllowNormalIsoforms() {
        String entryStr =
                "ID   NSMF-2_RAT              Reviewed;         509 AA.\n"
                        + "AC   Q9EPI6-2;\n"
                        + "CC   -!- ALTERNATIVE PRODUCTS:\n"
                        + "CC       Event=Alternative splicing; Named isoforms=5;\n"
                        + "CC         Comment=Additional isoforms seem to exist.;\n"
                        + "CC       Name=1;\n"
                        + "CC         IsoId=Q9EPI6-1; Sequence=Displayed;\n"
                        + "CC       Name=2;\n"
                        + "CC         IsoId=Q9EPI6-2; Sequence=VSP_014771;\n"
                        + "CC       Name=3;\n"
                        + "CC         IsoId=Q9EPI6-3; Sequence=VSP_014770;\n"
                        + "CC         Note=No experimental confirmation available.;\n"
                        + "CC       Name=4;\n"
                        + "CC         IsoId=Q9EPI6-4; Sequence=VSP_014771, VSP_014772;\n"
                        + "CC         Note=No experimental confirmation available.;\n"
                        + "CC       Name=5;\n"
                        + "CC         IsoId=Q9EPI6-5; Sequence=VSP_014773, VSP_014774;\n"
                        + "CC         Note=No experimental confirmation available.;";
        boolean result = ValidateUniProtKBSolrIndexMain.filterCanonicalIsoform(entryStr);
        assertTrue(result);
    }

    @Test
    void canAllowNormalEntries() {
        String entryStr =
                "ID   A0PHU1_9CICH            Unreviewed;       378 AA.\n" + "AC   A0PHU1;";
        boolean result = ValidateUniProtKBSolrIndexMain.filterCanonicalIsoform(entryStr);
        assertTrue(result);
    }
}
