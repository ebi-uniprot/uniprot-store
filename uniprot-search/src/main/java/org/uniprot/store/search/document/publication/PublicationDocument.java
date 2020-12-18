package org.uniprot.store.search.document.publication;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/**
 * @author sahmad
 * @created 04/12/2020
 */
@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PublicationDocument implements Document {

    // note: thinking to keep flat so that easier to have different release cycles in future
    @Field("id")
    private String id; // unique id composed of accession and pmid

    @Field("accession")
    private String accession;

    @Field("pubmed_id")
    private String pubMedId;

    @Field("computational_mapped_protein_count")
    private Long computationalMappedProteinCount;

    @Field("community_mapped_protein_count")
    private Long communityMappedProteinCount;

    @Field("unreviewed_mapped_protein_count")
    private Long unreviewedMappedProteinCount;

    @Field("reviewed_mapped_protein_count")
    private Long reviewedMappedProteinCount;

    @Field("is_large_scale")
    private boolean isLargeScale;

    @Field private Set<String> categories;

    @Field("types")
    private Set<Integer> types;

    // to be list of MappedReferences of different types viz. uniprot(reviewed/unreviewed),
    // community and computational
    @Field("publication_obj")
    private byte[] publicationMappedReferences;

    @Override
    public String getDocumentId() {
        return id;
    }
}
