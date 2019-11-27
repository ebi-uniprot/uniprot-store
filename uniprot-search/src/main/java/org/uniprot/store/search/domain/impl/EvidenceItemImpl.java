package org.uniprot.store.search.domain.impl;

import lombok.Data;
import lombok.NoArgsConstructor;

import org.uniprot.store.search.domain.EvidenceItem;

@Data
@NoArgsConstructor
public class EvidenceItemImpl implements EvidenceItem {

    private String name;
    private String code;
}
