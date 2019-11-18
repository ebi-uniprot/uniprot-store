package org.uniprot.store.search.domain2;

import java.util.List;

/**
 * Created 12/11/2019
 *
 * @author Edd
 */
public interface SearchItem {
    String getLabel();

    String getField();

    String getFieldValidRegex();

    String getIdField();

    String getIdValidRegex();

    String getSortField();

    String getAutoComplete();

    String getDataType();

    String getRangeField();

    String getEvidenceField();

    String getDescription();

    String getSites();

    String getExample();

    String getItemType();

    List<SearchItem> getItems();
}
