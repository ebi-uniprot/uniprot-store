package org.uniprot.store.indexer.arba;

import java.util.Set;

import lombok.Builder;
import lombok.Data;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
@Builder
@Data
public class ArbaDocumentComment {
    private String name; // comment display name
    private Set<String> values;
}
