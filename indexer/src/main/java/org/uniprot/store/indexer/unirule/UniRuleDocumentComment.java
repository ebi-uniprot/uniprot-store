package org.uniprot.store.indexer.unirule;

import java.util.Set;

import lombok.Builder;
import lombok.Data;

/**
 * @author sahmad
 * @date: 13 May 2020
 * model class to keep flattened comment object to be set in {{@link
 *     org.uniprot.store.search.document.unirule.UniRuleDocument}}'s comment and content fields
 */
@Builder
@Data
public class UniRuleDocumentComment {
    private String name;
    private Set<String> values;
}
