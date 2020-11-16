package org.uniprot.store.config.returnfield.factory;

import static java.util.stream.Collectors.groupingBy;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;

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

        String[] groups =
                config.getAllFields().stream()
                        .filter(field -> field.getItemType().equals(ReturnFieldItemType.GROUP))
                        .map(ReturnField::getId)
                        .toArray(String[]::new);
        Map<String, List<ReturnField>> groupToSingleFieldMap =
                config.getAllFields().stream()
                        .filter(field -> field.getItemType().equals(ReturnFieldItemType.SINGLE))
                        .collect(groupingBy(ReturnField::getParentId));

        // check the defined groups are the same as the union of all children's parents
        assertThat(groupToSingleFieldMap.keySet(), containsInAnyOrder(groups));

        // check each group has 1+ child
        groupToSingleFieldMap
                .values()
                .forEach((fields) -> assertThat(fields, hasSize(greaterThan(0))));
    }

    @Test
    void canCreateUniRuleReturnFieldConfig() {
        ReturnFieldConfig config =
                ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIRULE);
        assertThat(config, is(notNullValue()));
        assertThat(config.getAllFields(), hasSize(12));
        assertThat(config.getReturnFields(), hasSize(11));
    }
}
