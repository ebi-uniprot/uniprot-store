package org.uniprot.store.search.field;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Created 20/11/19
 *
 * @author Edd
 */
class SearchFieldsTest {
    @Test
    void hasField_isTrueWhenPresent() {
        assertThat(UniProtSearchFields.UNIPROTKB.hasField("accession"), is(true));
    }

    @Test
    void hasField_isFalseWhenNotPresent() {
        assertThat(UniProtSearchFields.UNIPROTKB.hasField("XXXXXXX"), is(false));
    }

    @Test
    void hasSortField_isTrueWhenPresent() {
        assertThat(UniProtSearchFields.UNIPROTKB.hasSortField("accession"), is(true));
    }

    @Test
    void hasSortField_isFalseWhenPresent() {
        assertThat(UniProtSearchFields.UNIPROTKB.hasSortField("mnemonic_default"), is(false));
    }

    @Test
    void getField_retrievesFieldWhenPresent() {
        assertThat(UniProtSearchFields.UNIPROTKB.getField("accession").getName(), is("accession"));
    }

    @Test
    void getField_throwsExceptionWhenNotPresent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> UniProtSearchFields.UNIPROTKB.getField("XXXXXXX"));
    }

    @Test
    void getSortFieldFor_retrievesSortFieldWhenPresent() {
        assertThat(
                UniProtSearchFields.UNIPROTKB.getSortFieldFor("accession").getName(),
                is("accession_id"));
    }

    @Test
    void getSortFieldFor_throwsExceptionWhenSortFieldNotPresent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> UniProtSearchFields.UNIPROTKB.getSortFieldFor("mnemonic_default"));
    }

    @Test
    void fieldValueIsValid_isTrueWhenValid() {
        assertThat(
                UniProtSearchFields.UNIPROTKB.fieldValueIsValid("accession", "P12345"), is(true));
    }

    @Test
    void fieldValueIsValid_isFalseWhenInvalid() {
        assertThat(
                UniProtSearchFields.UNIPROTKB.fieldValueIsValid("accession", "12345"), is(false));
    }
}
