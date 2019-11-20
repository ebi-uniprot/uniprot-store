package org.uniprot.store.search.document.uniref;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;

import lombok.*;
import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/**
 *
 * @author jluo
 * @date: 13 Aug 2019
 *
 */
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
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
	private Date created;
	@Singular
	@Field("uniprot_id")
	private List<String> uniprotIds;
	
    @Field("organism_sort")
    public String organismSort;
	
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
