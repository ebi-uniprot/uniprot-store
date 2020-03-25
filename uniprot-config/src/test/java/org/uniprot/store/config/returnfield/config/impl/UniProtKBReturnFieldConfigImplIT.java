package org.uniprot.store.config.returnfield.config.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;

/**
 * Created 17/03/20
 *
 * @author Edd
 */
class UniProtKBReturnFieldConfigImplIT {

    private static ReturnFieldConfig returnFieldConfig;
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void setUp() {
        returnFieldConfig =
                ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIPROTKB);
        searchFieldConfig =
                SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.UNIPROTKB);
    }

    @ParameterizedTest(name = "Sort field [{0}] for return field exists?")
    @MethodSource("provideSortFields")
    void validReturnFieldSortFieldDefined(String returnFieldsSortField) {
        assertThat(searchFieldConfig.correspondingSortFieldExists(returnFieldsSortField), is(true));
    }

    private static Stream<Arguments> provideSortFields() {
        return returnFieldConfig.getReturnFields().stream()
                .filter(field -> Utils.notNullNotEmpty(field.getSortField()))
                .map(ReturnField::getSortField)
                .map(Arguments::of);
    }
}
