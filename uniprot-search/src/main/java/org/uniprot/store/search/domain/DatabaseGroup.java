package org.uniprot.store.search.domain;

import java.util.List;

public interface DatabaseGroup {
	String getGroupName();
	List<Tuple> getItems();
}
