package org.uniprot.store.config.schema;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.model.FieldItem;
import org.uniprot.store.config.uniprotkb.UniProtSearchFieldConfiguration;

public class DataValidatorTest {
    private static final String TEST_SEARCH_FIELDS_CONFIG =
            "src/test/resources/test-uniprot-fields.json";

    @DisplayName("Test data validator")
    @Test
    void testDataValidator() {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        List<FieldItem> allFields = config.loadAndGetFieldItems(TEST_SEARCH_FIELDS_CONFIG);
        Map<String, FieldItem> idFieldMap = config.buildIdFieldItemMap(allFields);
        Assertions.assertDoesNotThrow(
                () -> DataValidator.validateContent(allFields, idFieldMap));
    }
}
