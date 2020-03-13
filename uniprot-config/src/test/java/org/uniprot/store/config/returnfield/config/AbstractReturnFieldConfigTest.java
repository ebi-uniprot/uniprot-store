package org.uniprot.store.config.returnfield.config;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;
import org.uniprot.store.config.returnfield.model.ReturnField;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created 13/03/20
 *
 * @author Edd
 */
class AbstractReturnFieldConfigTest {
    private static final String TEST_RETURN_FIELD_CONFIG_PATH = "test-return-fields.json";
    private static final String DYNAMICALLY_ADDED_ID = "ADDED DYNAMICALLY";
    private static FakeReturnFieldConfig config;

    @BeforeAll
    static void setUp() {
        config = new FakeReturnFieldConfig();
    }

    @Test
    void loadsExpectedGroups() {
        List<String> groupIds =
                config.getAllFields().stream()
                        .filter(field -> field.getItemType().equals(ReturnFieldItemType.GROUP))
                        .map(ReturnField::getId)
                        .collect(Collectors.toList());
        assertThat(
                groupIds,
                contains("names_&_taxonomy", "sequences", "protein_family/group", "other"));
    }

    @Test
    void loadsExpectedFields() {
        List<String> fieldIds =
                config.getReturnFields().stream()
                        .map(ReturnField::getId)
                        .collect(Collectors.toList());
        assertThat(
                fieldIds,
                contains(
                        "names_&_taxonomy/entry",
                        "names_&_taxonomy/entry_name",
                        "sequences/alternative_products_(isoforms)",
                        DYNAMICALLY_ADDED_ID));
    }

    @Test
    void canGetFieldByName() {
        ReturnField field = config.getReturnFieldByName("cc_alternative_products");
        assertThat(field, is(notNullValue()));
    }

    @Test
    void fetchingByNameThatDoesntExistCausesException() {
        assertThrows(IllegalArgumentException.class, () -> config.getReturnFieldByName("XXXX"));
    }

    @Test
    void fieldExists() {
        assertThat(config.returnFieldExists("cc_alternative_products"), is(true));
    }

    @Test
    void fieldDoesNotExist() {
        assertThat(config.returnFieldExists("XXXX"), is(false));
    }

    private static class FakeReturnFieldConfig extends AbstractReturnFieldConfig {
        FakeReturnFieldConfig() {
            super(TEST_RETURN_FIELD_CONFIG_PATH);
        }

        @Override
        protected Collection<ReturnField> dynamicallyLoadFields() {
            ReturnField field = new ReturnField();
            field.setId(DYNAMICALLY_ADDED_ID);
            field.setItemType(ReturnFieldItemType.SINGLE);
            return singletonList(field);
        }
    }
}
