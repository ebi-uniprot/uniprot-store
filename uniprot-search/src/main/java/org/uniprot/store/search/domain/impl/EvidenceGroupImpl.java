package org.uniprot.store.search.domain.impl;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

import org.uniprot.store.search.domain.EvidenceGroup;
import org.uniprot.store.search.domain.EvidenceItem;

@Data
@NoArgsConstructor
public class EvidenceGroupImpl implements EvidenceGroup {

	private String groupName;
	private List<EvidenceItem> items = new ArrayList<>();

}
