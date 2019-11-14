package org.uniprot.store.search.domain2;

import java.util.List;

/**
 * Created 14/11/19
 *
 * @author Edd
 */
public interface SearchFields {
    List<SearchItem> getSearchItems();
    List<SearchField> getSearchFields();
    List<SearchField> getTermFields();
    List<SearchField> getRangeFields();
    List<String> getSorts();
}

