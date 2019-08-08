package org.uniprot.store.search.domain;

import java.util.List;

public interface FieldGroup {
	String getGroupName();
	boolean isIsDatabase();
	List<Field> getFields();

}
