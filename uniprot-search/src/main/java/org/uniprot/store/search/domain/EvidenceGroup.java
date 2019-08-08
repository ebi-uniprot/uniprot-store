package org.uniprot.store.search.domain;

import java.util.List;

public interface EvidenceGroup {
	String getGroupName();
	List<EvidenceItem> getItems();
}
