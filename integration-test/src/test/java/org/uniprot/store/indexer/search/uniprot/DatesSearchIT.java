package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.search.field.QueryBuilder.after;
import static org.uniprot.store.search.field.QueryBuilder.before;
import static org.uniprot.store.search.field.QueryBuilder.rangeQuery;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;

/** Verifies if the creation and modification dates within the UniProt entry are indexed properly */
class DatesSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String DT_LINE =
            "DT   %s, integrated into UniProtKB/Swiss-Prot.\n"
                    + "DT   %s, sequence version 2.\n"
                    + "DT   %s, entry version 97.";
    private static final String ACCESSION1 = "Q197F4";
    private static final String CREATE_DATE1 = "01-OCT-1989";
    private static final String UPDATE_DATE1 = "07-FEB-2006";
    private static final String ACCESSION2 = "Q197F5";
    private static final String CREATE_DATE2 = "30-JUL-2003";
    private static final String UPDATE_DATE2 = "01-JAN-2013";
    private static final String ACCESSION3 = "Q197F6";
    private static final String CREATE_DATE3 = "15-MAR-1999";
    private static final String UPDATE_DATE3 = "27-OCT-2004";
    // entries that deliberately cross the GMT -> BST boundary
    // BST for 2014: 0100 @ 30 March - 0100 @ 26 October
    private static final String ACCESSION_GMT = "Q197F7";
    private static final String CREATE_DATE_GMT = "29-MAR-2014";
    private static final String UPDATE_DATE_GMT = "02-APR-2014";
    // this is dubiously BST because BST starts @ 1am on this day, and we specify no time
    private static final String ACCESSION_BST_DUBIOUS = "Q197F8";
    private static final String CREATE_DATE_BST_DUBIOUS = "30-MAR-2014";
    private static final String UPDATE_DATE_BST_DUBIOUS = "02-APR-2014";
    private static final String ACCESSION_BST = "Q197F9";
    private static final String CREATE_DATE_BST = "31-MAR-2014";
    private static final String UPDATE_DATE_BST = "02-APR-2014";
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(
                LineType.DT, String.format(DT_LINE, CREATE_DATE1, CREATE_DATE1, UPDATE_DATE1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(
                LineType.DT, String.format(DT_LINE, CREATE_DATE2, CREATE_DATE2, UPDATE_DATE2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(
                LineType.DT, String.format(DT_LINE, CREATE_DATE3, CREATE_DATE3, UPDATE_DATE3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 4
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION_BST));
        entryProxy.updateEntryObject(
                LineType.DT,
                String.format(DT_LINE, CREATE_DATE_BST, CREATE_DATE_BST, UPDATE_DATE_BST));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 5
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION_GMT));
        entryProxy.updateEntryObject(
                LineType.DT,
                String.format(DT_LINE, CREATE_DATE_GMT, CREATE_DATE_GMT, UPDATE_DATE_GMT));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 6
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION_BST_DUBIOUS));
        entryProxy.updateEntryObject(
                LineType.DT,
                String.format(
                        DT_LINE,
                        CREATE_DATE_BST_DUBIOUS,
                        CREATE_DATE_BST_DUBIOUS,
                        UPDATE_DATE_BST_DUBIOUS));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void searchForCreatedBefore30SEP1989Returns0Documents() {
        LocalDate creationDate = LocalDate.of(1989, 9, 30);

        String query =
                before(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        creationDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void searchForCreatedBefore01OCT1989Returns1Document() {
        LocalDate creationDate = LocalDate.of(1989, 10, 1);

        String query =
                before(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        creationDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void searchForCreatedBefore15MAR1999Returns2Documents() {
        LocalDate creationDate = LocalDate.of(1999, 3, 15);

        String query =
                before(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        creationDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Test
    void searchForUpdatedBefore26OCT2004Returns0Documents() {
        LocalDate updateDate = LocalDate.of(2004, 10, 26);

        String query =
                before(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        updateDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void searchForUpdatedBefore27OCT2004Returns1Documents() {
        LocalDate updateDate = LocalDate.of(2004, 10, 27);

        String query =
                before(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        updateDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void searchForUpdatedBefore08FEB2006Returns2Documents() {
        LocalDate updateDate = LocalDate.of(2006, 2, 8);

        String query =
                before(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        updateDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Disabled
    @Test
    void searchForCreatedAfter31MAR2014Returns1Document() {
        LocalDate creationDate = LocalDate.of(2014, 3, 30);

        String query =
                after(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        creationDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION_BST));
    }

    @Test
    void searchForCreatedAfter30JUL2003Returns3Documents() {
        LocalDate creationDate = LocalDate.of(2003, 7, 29);

        String query =
                after(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        creationDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(
                retrievedAccessions,
                containsInAnyOrder(
                        ACCESSION2, ACCESSION_BST, ACCESSION_GMT, ACCESSION_BST_DUBIOUS));
    }

    @Test
    void searchForCreatedAfter15MAR1999Returns2Documents() {
        LocalDate creationDate = LocalDate.of(1999, 3, 15);

        String query =
                after(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        creationDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(
                retrievedAccessions,
                containsInAnyOrder(
                        ACCESSION2,
                        ACCESSION3,
                        ACCESSION_BST,
                        ACCESSION_GMT,
                        ACCESSION_BST_DUBIOUS));
    }

    @Test
    void searchForUpdatedAfter1APR2014Returns3Documents() {
        LocalDate updateDate = LocalDate.of(2014, 4, 1);

        String query =
                after(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        updateDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(
                retrievedAccessions,
                containsInAnyOrder(ACCESSION_BST, ACCESSION_BST_DUBIOUS, ACCESSION_GMT));
    }

    @Test
    void searchForUpdatedAfter01JAN2013Returns3Documents() {
        LocalDate updateDate = LocalDate.of(2013, 1, 1);

        String query =
                after(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        updateDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(
                retrievedAccessions,
                containsInAnyOrder(
                        ACCESSION2, ACCESSION_BST, ACCESSION_GMT, ACCESSION_BST_DUBIOUS));
    }

    @Test
    void searchForUpdatedAfter07FEB2006Returns4Documents() {
        LocalDate updateDate = LocalDate.of(2006, 2, 7);

        String query =
                after(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        updateDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(
                retrievedAccessions,
                containsInAnyOrder(
                        ACCESSION1,
                        ACCESSION2,
                        ACCESSION_BST,
                        ACCESSION_GMT,
                        ACCESSION_BST_DUBIOUS));
    }

    @Test
    void searchCreationBetween20FEB1979And10Dec1979Returns0Documents() {
        LocalDate startDate = LocalDate.of(1979, 2, 20);
        LocalDate endDate = LocalDate.of(1979, 12, 10);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void createdBetween01JAN1989And01JAN2000ReturnsEntry1And3() {
        LocalDate startDate = LocalDate.of(1989, 1, 1);
        LocalDate endDate = LocalDate.of(2000, 1, 1);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Test
    void createdBetween01JAN1989And01JAN2000ReturnsEntry1And32() {
        LocalDate startDate = LocalDate.of(1989, 1, 1);
        LocalDate endDate = LocalDate.of(2000, 1, 1);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Test
    void searchUpdateBetween20FEB1979And10Dec1979Returns0Documents() {
        LocalDate startDate = LocalDate.of(1979, 2, 20);
        LocalDate endDate = LocalDate.of(1979, 12, 10);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void updatedBetween01JAN2004And01JAN2006ReturnsEntry1And3() {
        LocalDate startDate = LocalDate.of(2004, 1, 1);
        LocalDate endDate = LocalDate.of(2006, 12, 1);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Test
    void updatedBetween01JAN2004And01JAN2006ReturnsEntry1And32() {
        LocalDate startDate = LocalDate.of(2004, 1, 1);
        LocalDate endDate = LocalDate.of(2006, 12, 1);

        // String query = updated(startDate, endDate);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_modified")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    /*
     * BST for 2014:    30 March - 26 October
     * Entry created:   29 March 2014, therefore this date is GMT
     */
    @Test
    void searchExplicitGMTEntryTestUpperBound() {
        LocalDate startDate = LocalDate.of(2014, 3, 28);
        LocalDate endDate = LocalDate.of(2014, 3, 29);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION_GMT));
    }

    @Test
    void searchExplicitGMTEntryTestExactDay() {
        LocalDate startDate = LocalDate.of(2014, 3, 29);
        LocalDate endDate = LocalDate.of(2014, 3, 29);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION_GMT));
    }

    @Test
    void searchExplicitGMTEntryTestOver() {
        LocalDate startDate = LocalDate.of(2014, 3, 28);
        LocalDate endDate = LocalDate.of(2014, 3, 30);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION_BST_DUBIOUS, ACCESSION_GMT));
    }

    @Test
    void searchExplicitGMTEntryTestLowerBound() {
        LocalDate startDate = LocalDate.of(2014, 3, 29);
        LocalDate endDate = LocalDate.of(2014, 3, 30);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION_GMT, ACCESSION_BST_DUBIOUS));
    }

    /*
     * ---------------------------------------------------------------------------------------------
     * BST for 2014:    0100 @ 30 March - 0100 @ 26 October
     * Entry created:   31 March 2014, therefore this date is GMT
     */
    @Test
    void searchExplicitBSTEntryTestUpperBound() {
        LocalDate startDate = LocalDate.of(2014, 3, 30);
        LocalDate endDate = LocalDate.of(2014, 3, 31);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION_BST, ACCESSION_BST_DUBIOUS));
    }

    @Disabled
    @Test
    void searchExplicitBSTEntryTestExactDay() {
        LocalDate startDate = LocalDate.of(2014, 3, 31);
        LocalDate endDate = LocalDate.of(2014, 3, 31);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, contains(ACCESSION_BST));
    }

    @Test
    void searchExplicitBSTEntryTestOver() {
        LocalDate startDate = LocalDate.of(2014, 3, 30);
        LocalDate endDate = LocalDate.of(2014, 4, 1);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION_BST, ACCESSION_BST_DUBIOUS));
    }

    @Disabled
    @Test
    void searchExplicitBSTEntryTestLowerBound() {
        LocalDate startDate = LocalDate.of(2014, 3, 31);
        LocalDate endDate = LocalDate.of(2014, 4, 1);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION_BST));
    }

    /*
     * BST for 2014:    0100 @ 30 March - 0100 @ 26 October
     * Entry created:   30 March 2014, therefore this date is GMT still, since it starts @ midnight
     */
    @Test
    void searchExplicitBSTDubiousEntryTestUpperBound() {
        LocalDate startDate = LocalDate.of(2014, 3, 29);
        LocalDate endDate = LocalDate.of(2014, 3, 30);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION_GMT, ACCESSION_BST_DUBIOUS));
    }

    @Test
    void searchExplicitBSTDubiousEntryTestExactDay() {
        LocalDate startDate = LocalDate.of(2014, 3, 30);
        LocalDate endDate = LocalDate.of(2014, 3, 30);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION_BST_DUBIOUS));
    }

    @Test
    void searchExplicitBSTDubiousEntryTestOver() {
        LocalDate startDate = LocalDate.of(2014, 3, 29);
        LocalDate endDate = LocalDate.of(2014, 3, 31);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(
                retrievedAccessions,
                containsInAnyOrder(ACCESSION_BST_DUBIOUS, ACCESSION_GMT, ACCESSION_BST));
    }

    @Test
    void searchExplicitBSTDubiousEntryTestLowerBound() {
        LocalDate startDate = LocalDate.of(2014, 3, 30);
        LocalDate endDate = LocalDate.of(2014, 3, 31);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_created")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION_BST_DUBIOUS, ACCESSION_BST));
    }

    @Test
    void searchExplicitBSTSequenceUpdateTestOver() {
        LocalDate startDate = LocalDate.of(2014, 3, 30);
        LocalDate endDate = LocalDate.of(2014, 4, 1);

        String query =
                rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("date_sequence_modified")
                                .getFieldName(),
                        startDate,
                        endDate);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION_BST, ACCESSION_BST_DUBIOUS));
    }
}
