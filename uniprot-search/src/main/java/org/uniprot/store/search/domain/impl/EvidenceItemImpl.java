package org.uniprot.store.search.domain.impl;

import org.uniprot.store.search.domain.EvidenceItem;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EvidenceItemImpl implements EvidenceItem {

    private String name;
    private String code;
}
