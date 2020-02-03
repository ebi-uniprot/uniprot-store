package org.uniprot.store.search.field;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The purpose of this test class is to ensure all the configuration files for the search fields can
 * be loaded correctly. The contents of the tests need not be comprehensive, just check that search
 * fields exist.
 *
 * <p>Created 17/01/2020
 *
 * @author Edd
 */
class UniProtSearchFieldsTest {
    @ParameterizedTest
    @EnumSource(UniProtSearchFields.class)
    void canLoadFields(UniProtSearchFields searchFields) {
        assertThat(searchFields.getSearchFields(), hasSize(greaterThan(0)));
        assertThat(searchFields.getSearchFields(), is(notNullValue()));
    }
}
