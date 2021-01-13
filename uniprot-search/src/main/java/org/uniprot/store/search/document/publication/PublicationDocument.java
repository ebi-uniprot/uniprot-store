package org.uniprot.store.search.document.publication;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Singular;

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

    private static final long serialVersionUID = 1401052603497411363L;
    // note: thinking to keep flat so that easier to have different release cycles in future
    @Field("id")
    private String id; // guid

    @Field("accession")
    private String accession;

    @Field("pubmed_id")
    private String pubMedId;

    private Set<String> categories = new HashSet<>();

    @Singular private Set<Integer> types = new HashSet<>();

    // TODO: 06/01/2021 maybe remove all counts from this doc, and put inside literature document
    // (so that counts are associated with the publication)
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

    @Field("publication_obj")
    private byte[] publicationMappedReferences;

    @Field("reference_number")
    private Integer refNumber;

    @Field("main_type")
    private Integer mainType;

    @Override
    public String getDocumentId() {
        return id;
    }

    @Field("categories")
    public void setCategories(List<String> categories) {
        this.categories = new HashSet<>(categories);
    }

    @Field("types")
    public void setTypes(List<Integer> types) {
        this.types = new HashSet<>(types);
    }
}
