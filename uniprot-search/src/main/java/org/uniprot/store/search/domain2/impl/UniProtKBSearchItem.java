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
public class UniProtKBSearchItem implements SearchItem {
    String id;
    String label;
    String term;
    String termValidRegex;
    String idValidRegex;
    String idTerm;
    String sortTerm;
    String autoComplete;
    String dataType = "string";
    String rangeTerm;
    String evTerm;
    String description;
    String sites;
    String example;
    String itemType = "single";
    List<SearchItem> items;
}
