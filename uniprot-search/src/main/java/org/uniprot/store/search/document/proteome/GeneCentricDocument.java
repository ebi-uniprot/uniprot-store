package org.uniprot.store.search.document.proteome;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/**
 * @author jluo
 * @date: 16 May 2019
 */
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class GeneCentricDocument implements Document {
    private static final long serialVersionUID = -1586599109792615114L;

    @Field("accession_id")
    private String accession;

    @Field("accession")
    private List<String> accessions = new ArrayList<>();

    @Field("reviewed")
    private Boolean reviewed;

    @Field("gene")
    private List<String> geneNames = new ArrayList<>();

    // DEFAULT SEARCH FIELD
    @Field("content")
    private Set<String> content = new HashSet<>();

    @Field("upid")
    private String upid;

    @Field("organism_id")
    private int organismTaxId;

    @Field("genecentric_stored")
    private byte[] geneCentricStored;

    @Override
    public String getDocumentId() {
        return accession;
    }
}
