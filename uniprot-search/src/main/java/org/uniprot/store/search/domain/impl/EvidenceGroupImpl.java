package org.uniprot.store.search.domain.impl;

import java.util.ArrayList;
import java.util.List;

import org.uniprot.store.search.domain.EvidenceGroup;
import org.uniprot.store.search.domain.EvidenceItem;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EvidenceGroupImpl implements EvidenceGroup {

    private String groupName;
    private List<EvidenceItem> items = new ArrayList<>();
}
