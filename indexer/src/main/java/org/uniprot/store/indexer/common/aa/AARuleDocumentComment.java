package org.uniprot.store.indexer.common.aa;

import java.util.Set;

import lombok.Builder;
import lombok.Data;

/**
 * @author sahmad
 * @date: 13 May 2020 model class to keep flattened comment object to be set in {{@link
 *     org.uniprot.store.search.document.unirule.UniRuleDocument}}'s comment and content fields
 */
@Builder
@Data
public class AARuleDocumentComment {
    private String name; // comment display name
    private Set<String> values;
    private Set<String> notes; // special case for cofactor/subcellular_location with notes
    private Set<String> families; // special case for type SIMILARITY
}
