package org.uniprot.store.config.returnfield.factory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.common.ReturnFieldConfig;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created 05/03/2020
 *
 * @author Edd
 */
class ReturnFieldConfigFactoryTest {
    @Test
    void canCreateUniProtKBReturnFieldConfig() {
        ReturnFieldConfig config =
                ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIPROTKB);
        assertThat(config, is(notNullValue()));
        assertThat(config.getAllFields(), hasSize(greaterThan(0)));
        assertThat(config.getReturnFields(), hasSize(greaterThan(0)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("provideInvalidUniProtDataTypes")
    void invalidUniProtDataTypeParameterCausesException(UniProtDataType invalidType) {
        assertThrows(
                IllegalArgumentException.class,
                () -> ReturnFieldConfigFactory.getReturnFieldConfig(invalidType));
    }

    private static Stream<Arguments> provideInvalidUniProtDataTypes() {
        return Arrays.stream(UniProtDataType.values())
                .filter(type -> type != UniProtDataType.UNIPROTKB)
                .map(Arguments::of);
    }
}
