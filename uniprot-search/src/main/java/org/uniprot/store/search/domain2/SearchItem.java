package org.uniprot.store.search.domain2;

import java.util.List;

/**
 * Represents search items and their associated meta-data. Instances of this class form the basis of
 * all {@link SearchField}s (fields used by the search engine), regular expressions for valid
 * values, and human readable descriptions (used by front-end and other clients).
 *
 * <p>Created 12/11/2019
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

    String getExample();

    String getItemType();

    List<SearchItem> getItems();
}
