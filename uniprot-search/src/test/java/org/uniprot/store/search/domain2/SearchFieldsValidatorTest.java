package org.uniprot.store.search.domain2;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created 20/11/2019
 *
 * @author Edd
 */
class SearchFieldsValidatorTest {
    @Test
    void duplicateFieldsCauseException() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        SearchFieldsValidator.validate(
                                asList(
                                        SearchFieldImpl.builder()
                                                .name("field")
                                                .type(SearchFieldType.GENERAL)
                                                .build(),
                                        SearchFieldImpl.builder()
                                                .name("field")
                                                .type(SearchFieldType.GENERAL)
                                                .build())));
    }

    @Test
    void missingTypeCausesException() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        SearchFieldsValidator.validate(
                                asList(
                                        SearchFieldImpl.builder()
                                                .name("field1")
                                                .type(SearchFieldType.GENERAL)
                                                .build(),
                                        SearchFieldImpl.builder().name("field2").build())));
    }

    @Test
    void missingFieldCausesException() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        SearchFieldsValidator.validate(
                                asList(
                                        SearchFieldImpl.builder()
                                                .type(SearchFieldType.GENERAL)
                                                .build(),
                                        SearchFieldImpl.builder().name("field2").build())));
    }

    @Test
    void sortWithoutFieldCausesException() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        SearchFieldsValidator.validate(
                                asList(
                                        SearchFieldImpl.builder()
                                                .sortName("sortField")
                                                .type(SearchFieldType.GENERAL)
                                                .build(),
                                        SearchFieldImpl.builder()
                                                .name("field2")
                                                .type(SearchFieldType.GENERAL)
                                                .build())));
    }

    @Test
    void fieldSortAndRangeCanBeTheSame() {
        boolean expectTrue = true;
        SearchFieldsValidator.validate(
                asList(
                        SearchFieldImpl.builder()
                                .name("field1")
                                .sortName("field1")
                                .type(SearchFieldType.GENERAL)
                                .build(),
                        SearchFieldImpl.builder()
                                .name("field2")
                                .type(SearchFieldType.GENERAL)
                                .build()));
        assertTrue(expectTrue);
    }
}
