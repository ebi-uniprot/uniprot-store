package org.uniprot.store.search.domain2;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;

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
                                                .sortField(
                                                        SearchFieldImpl.builder()
                                                                .name("sortField")
                                                                .type(SearchFieldType.GENERAL)
                                                                .build())
                                                .type(SearchFieldType.GENERAL)
                                                .build(),
                                        SearchFieldImpl.builder()
                                                .name("sortField")
                                                .type(SearchFieldType.GENERAL)
                                                .build())));
    }

    @Test
    void fieldSortAndNameCanBeTheSame() {
        boolean expectTrue = true;
        SearchFieldsValidator.validate(
                asList(
                        SearchFieldImpl.builder()
                                .name("field1")
                                .sortField(
                                        SearchFieldImpl.builder()
                                                .name("field1")
                                                .type(SearchFieldType.GENERAL)
                                                .build())
                                .type(SearchFieldType.GENERAL)
                                .build(),
                        SearchFieldImpl.builder()
                                .name("field2")
                                .type(SearchFieldType.GENERAL)
                                .build()));
        assertTrue(expectTrue);
    }

    @Test
    void sameFieldForGeneralAndRangeCausesDuplicateException() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        SearchFieldsValidator.validate(
                                asList(
                                        SearchFieldImpl.builder()
                                                .name("field1")
                                                .type(SearchFieldType.GENERAL)
                                                .build(),
                                        SearchFieldImpl.builder()
                                                .name("field1")
                                                .type(SearchFieldType.RANGE)
                                                .build())));
    }
}
