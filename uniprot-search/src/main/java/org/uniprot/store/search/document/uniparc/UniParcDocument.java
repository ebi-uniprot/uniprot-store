package org.uniprot.store.search.document.uniparc;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Builder
@Getter
public class UniParcDocument implements Document {
    
	@Field("upi")
	private String upi;
	
	@Field("checksum")
	private String sequenceChecksum;
	
    @Field("length")
	private int seqLength;
 
	@Field("database")
	   @Singular
	private Set<String> databases ;
	  @Singular
	@Field("active")
	private Set<String> actives ;
	  @Singular
	@Field("gene")
	private Set<String> geneNames ;
	  @Singular
	@Field("protein")
	private Set<String> proteinNames ;

	  @Singular
	@Field("upid")
	private List<String> upids ;
	  @Singular
    @Field("taxonomy_name")
    private List<String> organismTaxons;
	  @Singular
    @Field("taxonomy_id")
    private List<Integer> taxLineageIds ;
	
	  @Singular
	@Field("accession")
	private List<String> uniprotAccessions ;
	  @Singular
	@Field("isoform")
	private List<String> uniprotIsoforms ;
	
    @Field("entry_stored")
    private ByteBuffer entryStored;
    
    //DEFAULT SEARCH FIELD
    @Field("content")
    public List<String> content ;

	@Override
	public String getDocumentId() {
		return upi;
	}
    
  
}