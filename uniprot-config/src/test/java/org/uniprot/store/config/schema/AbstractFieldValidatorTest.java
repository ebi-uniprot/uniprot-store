package org.uniprot.store.config.schema;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.model.Field;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created 11/03/2020
 *
 * @author Edd
 */
class AbstractFieldValidatorTest {
    private static FakeValidator validator;

    @BeforeAll
    static void globalSetUp() {
        validator = new FakeValidator();
    }

    @Test
    void testParentIdIsValidId() {
        FakeFieldItem fi1 = getFieldItem("id1", "parentId1");
        FakeFieldItem fi2 = getFieldItem("id2", "parentId2");
        FakeFieldItem fi3 = getFieldItem("id3", null);
        FakeFieldItem p1 = getFieldItem("parentId1", null);
        FakeFieldItem p2 = getFieldItem("parentId2", null);
        List<FakeFieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, p1, p2);
        Set<String> ids = validator.extractIds(fieldItems);
        assertDoesNotThrow(() -> validator.validateParentExists(fieldItems, ids));
    }

    @Test
    void testFieldItemsSeqNumbers() {
        FakeFieldItem fi1 = getFieldItem("id1", 0);
        FakeFieldItem fi2 = getFieldItem("id2", 1);
        FakeFieldItem fi3 = getFieldItem("id3", 2);
        FakeFieldItem fi4 = getFieldItem("id4", 3);
        List<FakeFieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, fi4);
        assertDoesNotThrow(() -> validator.validateSeqNumbers(fieldItems));
    }

    @Test
    void fieldItemsSeqNumbersNotContiguous() {
        FakeFieldItem fi1 = getFieldItem("id1", 0);
        FakeFieldItem fi2 = getFieldItem("id2", 1);
        FakeFieldItem fi3 = getFieldItem("id3", 2);
        FakeFieldItem fi4 = getFieldItem("id4", 4);
        List<FakeFieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, fi4);
        assertThrows(
                SchemaValidationException.class, () -> validator.validateSeqNumbers(fieldItems));
    }

    @Test
    void testFieldItemsChildNumbers() {
        FakeFieldItem p1 = getFieldItem("p1", null);
        FakeFieldItem p1Ch1 = getFieldItem("p1ch1", "p1");
        FakeFieldItem p1Ch2 = getFieldItem("p1ch2", "p1");
        FakeFieldItem p1Ch3 = getFieldItem("p1ch3", "p1");
        p1Ch1.setChildNumber(0);
        p1Ch2.setChildNumber(2);
        p1Ch3.setChildNumber(1);
        List<FakeFieldItem> fieldItems = Arrays.asList(p1, p1Ch1, p1Ch2, p1Ch3);
        assertDoesNotThrow(() -> validator.validateChildNumbers(fieldItems));
    }

    @Test
    void testParentIdIsInvalidId() {
        // create few fields with non-existing parentId
        FakeFieldItem fi1 = getFieldItem("id1", "parentId");
        FakeFieldItem fi2 = getFieldItem("id2", "invalidParentId");
        FakeFieldItem fi3 = getFieldItem("id3", null);
        FakeFieldItem p1 = getFieldItem("parentId", null);
        List<FakeFieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, p1);
        Set<String> ids = validator.extractIds(fieldItems);
        assertThrows(
                SchemaValidationException.class,
                () -> validator.validateParentExists(fieldItems, ids));
    }

    @Test
    void testNegativeSeqNumber() {
        FakeFieldItem fi1 = getFieldItem("id1", 0);
        FakeFieldItem fi2 = getFieldItem("id2", 1);
        FakeFieldItem fi3 = getFieldItem("id3", 2);
        FakeFieldItem fi4 = getFieldItem("id4", -3);
        List<FakeFieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, fi4);
        assertThrows(
                SchemaValidationException.class, () -> validator.validateSeqNumbers(fieldItems));
    }

    @Test
    void testFieldItemsMissingChildNumbers() {
        FakeFieldItem p1 = getFieldItem("p1", null);
        FakeFieldItem p1Ch1 = getFieldItem("p1ch1", "p1");
        FakeFieldItem p1Ch2 = getFieldItem("p1ch2", "p1");
        FakeFieldItem p1Ch3 = getFieldItem("p1ch3", "p1");
        p1Ch1.setChildNumber(0);
        p1Ch3.setChildNumber(1);
        List<FakeFieldItem> fieldItems = Arrays.asList(p1, p1Ch1, p1Ch2, p1Ch3);
        assertThrows(
                SchemaValidationException.class, () -> validator.validateChildNumbers(fieldItems));
    }

    @Test
    void testFieldItemsInvalidChildNumbersSequence() {
        FakeFieldItem p1 = getFieldItem("p1", null);
        FakeFieldItem p1Ch1 = getFieldItem("p1ch1", "p1");
        FakeFieldItem p1Ch2 = getFieldItem("p1ch2", "p1");
        FakeFieldItem p1Ch3 = getFieldItem("p1ch3", "p1");
        p1Ch1.setChildNumber(0);
        p1Ch2.setChildNumber(3);
        p1Ch3.setChildNumber(1);
        List<FakeFieldItem> fieldItems = Arrays.asList(p1, p1Ch1, p1Ch2, p1Ch3);
        assertThrows(
                SchemaValidationException.class, () -> validator.validateChildNumbers(fieldItems));
    }

    private FakeFieldItem getFieldItem(String id, int seqNumber) {
        SearchFieldItemType itemType = SearchFieldItemType.GROUP;
        String label = "Dummy Label";
        FakeFieldItem fi = getFieldItem(id, "");
        fi.setSeqNumber(seqNumber);
        fi.setItemType(itemType);
        fi.setLabel(label);
        return fi;
    }

    private FakeFieldItem getFieldItem(String id, String parentId) {
        FakeFieldItem fi = new FakeFieldItem();
        fi.setId(id);
        if (StringUtils.isNotEmpty(parentId)) {
            fi.setParentId(parentId);
        }
        return fi;
    }

    private static class FakeValidator extends AbstractFieldValidator<FakeFieldItem> {
        @Override
        public void validateContent(List<FakeFieldItem> fieldItems) {
            throw new IllegalStateException(
                    "Not testing this method, because it is an aggregate of behaviour being tested");
        }
    }

    @Data
    private static class FakeFieldItem implements Field, Serializable {
        private static final long serialVersionUID = -1835246966684124878L;
        @NotNull private String id;
        private String parentId;
        @PositiveOrZero private Integer childNumber;
        @NotNull @PositiveOrZero private Integer seqNumber;
        @NotNull private SearchFieldItemType itemType;
        private SearchFieldType fieldType;
        private String label;
        private String fieldName;
        private SearchFieldDataType dataType;
        private String description;
        private String example;
        private String validRegex;
        private List<org.uniprot.store.config.searchfield.model.SearchFieldItem.Value> values;
        private String autoComplete;
        private String autoCompleteQueryField;
        private String autoCompleteQueryFieldValidRegex;
        private String sortFieldId;
    }
}
