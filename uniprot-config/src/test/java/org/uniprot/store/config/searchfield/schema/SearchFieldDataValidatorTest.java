package org.uniprot.store.config.searchfield.schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.schema.SchemaValidationException;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchFieldDataValidatorTest {
    public static final String SORT_ID_0 = "sortId0";
    public static final String SORT_ID_1 = "sortId1";
    public static final String SORT_ID_2 = "sortId2";
    private final SearchFieldDataValidator searchFieldDataValidator = new SearchFieldDataValidator();
    private final SearchFieldItem searchFieldItem0 = new SearchFieldItem();
    private final SearchFieldItem searchFieldItem1 = new SearchFieldItem();
    private final SearchFieldItem searchFieldItem2 = new SearchFieldItem();
    private final SearchFieldItem searchFieldItem3 = new SearchFieldItem();

    @Test
    void extractParentNodes() {
        searchFieldItem0.setItemType(SearchFieldItemType.GROUP);
        searchFieldItem1.setItemType(SearchFieldItemType.SIBLING_GROUP);
        searchFieldItem2.setItemType(SearchFieldItemType.SINGLE);

        List<SearchFieldItem> extracted = searchFieldDataValidator.extractParentNodes(List.of(searchFieldItem0, searchFieldItem1, searchFieldItem2, searchFieldItem3));

        assertEquals(2, extracted.size());
        assertThat(extracted, containsInAnyOrder(searchFieldItem0, searchFieldItem1));
    }

    @Test
    void validateContent() {
        searchFieldItem0.setSortFieldId(SORT_ID_0);
        searchFieldItem0.setId(SORT_ID_0);
        searchFieldItem1.setSortFieldId(SORT_ID_1);
        searchFieldItem1.setId(SORT_ID_1);
        searchFieldItem2.setSortFieldId(SORT_ID_2);
        searchFieldItem2.setId(SORT_ID_2);
        searchFieldItem3.setValues(List.of(new SearchFieldItem.Value()));
        searchFieldItem3.setDataType(SearchFieldDataType.ENUM);

        searchFieldDataValidator.validateContent(List.of(searchFieldItem0, searchFieldItem1, searchFieldItem2, searchFieldItem3));
    }

    @Test
    void validateContent_invalidSortId() {
        searchFieldItem0.setSortFieldId(SORT_ID_0);
        searchFieldItem0.setId("invalid");
        searchFieldItem1.setSortFieldId(SORT_ID_1);
        searchFieldItem1.setId(SORT_ID_1);
        searchFieldItem2.setSortFieldId(SORT_ID_2);
        searchFieldItem2.setId(SORT_ID_2);
        searchFieldItem3.setValues(List.of(new SearchFieldItem.Value()));
        searchFieldItem3.setDataType(SearchFieldDataType.ENUM);

        try {
            searchFieldDataValidator.validateContent(List.of(searchFieldItem0, searchFieldItem1, searchFieldItem2, searchFieldItem3));
        } catch (SchemaValidationException e) {
            assertTrue(e.getMessage().contains("sortId"));
        }
    }

    @Test
    void validateContent_invalidFieldDataType() {
        searchFieldItem0.setSortFieldId(SORT_ID_0);
        searchFieldItem0.setId(SORT_ID_0);
        searchFieldItem1.setSortFieldId(SORT_ID_1);
        searchFieldItem1.setId(SORT_ID_1);
        searchFieldItem2.setSortFieldId(SORT_ID_2);
        searchFieldItem2.setId(SORT_ID_2);
        searchFieldItem3.setValues(List.of(new SearchFieldItem.Value()));
        searchFieldItem3.setDataType(SearchFieldDataType.STRING);

        try {
            searchFieldDataValidator.validateContent(List.of(searchFieldItem0, searchFieldItem1, searchFieldItem2, searchFieldItem3));
        } catch (SchemaValidationException e) {
            assertTrue(e.getMessage().contains("ENUM"));
        }
    }
}