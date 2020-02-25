package org.uniprot.store.search.field;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.factory.UniProtDataType;

/**
 * Created 20/11/19
 *
 * @author Edd
 */
class SearchFieldsTest {
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void setUp() {
        searchFieldConfig =
                SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.uniprotkb);
    }

    @Test
    void hasField_isTrueWhenPresent() {
        assertThat(searchFieldConfig.hasSearchFieldItem("accession"), is(true));
    }

    @Test
    void hasField_isFalseWhenNotPresent() {
        assertThat(searchFieldConfig.hasSearchFieldItem("XXXXXXX"), is(false));
    }

    @Test
    void hasSortField_isTrueWhenPresent() {
        assertThat(searchFieldConfig.hasCorrespondingSortField("accession"), is(true));
    }

    @Test
    void hasSortField_isFalseWhenPresent() {
        assertThat(searchFieldConfig.hasCorrespondingSortField("mnemonic_default"), is(false));
    }

    @Test
    void getField_retrievesFieldWhenPresent() {
        assertThat(
                searchFieldConfig.getSearchFieldItemByName("accession").getFieldName(),
                is("accession"));
    }

    @Test
    void getField_throwsExceptionWhenNotPresent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> searchFieldConfig.getSearchFieldItemByName("XXXXXXX"));
    }

    @Test
    void getSortFieldFor_retrievesSortFieldWhenPresent() {
        assertThat(
                searchFieldConfig.getCorrespondingSortField("accession").getFieldName(),
                is("accession_id"));
    }

    @Test
    void getSortFieldFor_throwsExceptionWhenSortFieldNotPresent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> searchFieldConfig.getCorrespondingSortField("mnemonic_default"));
    }

    @Test
    void fieldValueIsValid_isTrueWhenValid() {
        assertThat(searchFieldConfig.isSearchFieldValueValid("accession", "P12345"), is(true));
    }

    @Test
    void fieldValueIsValid_isFalseWhenInvalid() {
        assertThat(searchFieldConfig.isSearchFieldValueValid("accession", "12345"), is(false));
    }
}
