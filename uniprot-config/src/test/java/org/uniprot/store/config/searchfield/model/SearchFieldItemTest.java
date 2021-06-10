package org.uniprot.store.config.searchfield.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.uniprot.store.config.searchfield.model.SearchFieldItem.CONTEXT_PATH_TOKEN;

/**
 * @author sahmad
 * @created 10/06/2021
 */
class SearchFieldItemTest {

    @Test
    void testGetAutoCompleteWithNullValue(){
        SearchFieldItem item = new SearchFieldItem();
        Assertions.assertNull(item.getAutoComplete());
    }

    @Test
    void testGetAutoComplete(){
        SearchFieldItem item = new SearchFieldItem();
        String sampleAutoComplete = "sample/url";
        item.setAutoComplete(sampleAutoComplete);
        Assertions.assertEquals(sampleAutoComplete, item.getAutoComplete());
    }

    @Test
    void testGetAutoCompleteWithContext(){
        SearchFieldItem item = new SearchFieldItem();
        String sampleAutoComplete = CONTEXT_PATH_TOKEN + "/sample/url";
        item.setAutoComplete(sampleAutoComplete);
        String contextPathValue = "context/path";
        String actualPath = contextPathValue + "/sample/url";
        Assertions.assertEquals(actualPath, item.getAutoComplete(contextPathValue));
    }
}
