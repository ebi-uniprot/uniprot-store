package org.uniprot.store.search.document.uniref;

import java.time.LocalDate;
import java.util.List;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/**
 *
 * @author jluo
 * @date: 13 Aug 2019
 *
 */
@Builder
@Getter
public class UniRefDocument implements Document {

	@Field("id")
	private String id;

	@Field("name")
	private String name;

	@Field("identity")
	private String identity;

	@Field("count")
	private int count;

	@Field("length")
	private int length;

	@Field("created")
	private LocalDate created;
	@Singular
	@Field("uniprotid")
	private List<String> uniprotIds;
	
	@Field("upi")
	private List<String> upis;
	
	@Singular
	@Field("taxonomy_name")
	private List<String> organismTaxons;
	@Singular
	@Field("taxonomy_id")
	private List<Integer> taxLineageIds;
	// DEFAULT SEARCH FIELD
	@Field("content")
	public List<String> content;

	@Override
	public String getDocumentId() {
		return id;
	}

}
