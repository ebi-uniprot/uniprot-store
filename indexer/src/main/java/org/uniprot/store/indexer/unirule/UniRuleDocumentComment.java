package org.uniprot.store.indexer.unirule;

import java.util.Collection;

import lombok.Builder;
import lombok.Data;

/**
 * @author sahmad
 * @date: 13 May 2020 model class to keep flattened comment object to be set in
 *     unirulesolrdocument's comment and content fields
 */
@Builder
@Data
public class UniRuleDocumentComment {
    private String name;
    private Collection<String> values;
    private String stringValue; // for content field
}
