package org.uniprot.store.search.field;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.uniprot.store.search.domain.Field;
import org.uniprot.store.search.domain.FieldGroup;
import org.uniprot.store.search.domain.impl.FieldGroupImpl;
import org.uniprot.store.search.domain.impl.FieldImpl;
import org.uniprot.store.search.domain.impl.JsonConfig;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author jluo
 * @date: 20 Aug 2019
 *
*/

public enum UniRefResultFields implements ReturnField {
	INSTANCE;
	private static final String FILENAME = "uniref/uniref_result_field.json";
	private List<FieldGroup> resultFields = new ArrayList<>();
	private Map<String, Field> fieldMap = new HashMap<>();
	

	UniRefResultFields() {
		init();
	}

	void init() {
		ObjectMapper objectMapper = JsonConfig.getJsonMapper();
		try (InputStream is = ProteomeResultFields.class.getClassLoader().getResourceAsStream(FILENAME);) {
			List<FieldGroupImpl> fields = objectMapper.readValue(is,
					new TypeReference<List<FieldGroupImpl>>() {
					});
			this.resultFields.addAll(fields);
			
			this.fieldMap.put("upi", new FieldImpl("Entry", "upi"));		
			this.resultFields.stream().flatMap(val -> val.getFields().stream())
					.forEach(field -> this.fieldMap.put(field.getName(), field));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public List<FieldGroup> getResultFieldGroups() {
		return resultFields;
	}

	public Optional<Field> getField(String name) {
		return Optional.ofNullable( this.fieldMap.get(name));
	}

	public Map<String, Field> getAllFields(){
		return fieldMap;
	}
	@Override
	public String toString() {
		return resultFields.stream().map(val -> val.toString()).collect(Collectors.joining(",\n"));
	}

	@Override
	public boolean hasReturnField(String fieldName) {
		return INSTANCE.getField(fieldName).isPresent();
	}
}

