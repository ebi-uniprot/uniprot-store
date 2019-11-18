package org.uniprot.store.search.domain2.impl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.ToString;
import org.uniprot.store.search.domain2.SearchItem;

import java.util.List;

/**
 * Created 14/11/19
 *
 * @author Edd
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@ToString
public class SearchItemImpl implements SearchItem {
    String id;
    String label;
    String field;
    String fieldValidRegex;
    String idField;
    String idValidRegex;
    String sortField;
    String autoComplete;
    String dataType = "string";
    String rangeField;
    String evField;
    String description;
    String sites;
    String example;
    String itemType = "single";
    List<SearchItem> items;
}
