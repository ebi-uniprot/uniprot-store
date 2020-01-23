package org.uniprot.store.search.domain2;

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
        assertThat(UniProtKBSearchFields.INSTANCE.hasField("accession"), is(true));
    }

    @Test
    void hasField_isFalseWhenNotPresent() {
        assertThat(UniProtKBSearchFields.INSTANCE.hasField("XXXXXXX"), is(false));
    }

    @Test
    void hasSortField_isTrueWhenPresent() {
        assertThat(UniProtKBSearchFields.INSTANCE.hasSortField("accession"), is(true));
    }

    @Test
    void hasSortField_isFalseWhenPresent() {
        assertThat(UniProtKBSearchFields.INSTANCE.hasSortField("mnemonic_default"), is(false));
    }

    @Test
    void getField_retrievesFieldWhenPresent() {
        assertThat(UniProtKBSearchFields.INSTANCE.getField("accession").getName(), is("accession"));
    }

    @Test
    void getField_throwsExceptionWhenNotPresent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> UniProtKBSearchFields.INSTANCE.getField("XXXXXXX"));
    }

    @Test
    void getSortFieldFor_retrievesSortFieldWhenPresent() {
        assertThat(
                UniProtKBSearchFields.INSTANCE.getSortFieldFor("accession").getName(),
                is("accession_id"));
    }

    @Test
    void getSortFieldFor_throwsExceptionWhenSortFieldNotPresent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> UniProtKBSearchFields.INSTANCE.getSortFieldFor("mnemonic_default"));
    }

    @Test
    void fieldValueIsValid_isTrueWhenValid() {
        assertThat(
                UniProtKBSearchFields.INSTANCE.fieldValueIsValid("accession", "P12345"), is(true));
    }

    @Test
    void fieldValueIsValid_isFalseWhenInvalid() {
        assertThat(
                UniProtKBSearchFields.INSTANCE.fieldValueIsValid("accession", "12345"), is(false));
    }
}
