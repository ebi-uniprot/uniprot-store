package org.uniprot.store.search.document.unirule;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/** @author sahmad */
@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class UniRuleDocument implements Document {
    @Field("unirule_id")
    private String uniRuleId;

    @Field("condition_value")
    private Set<String> conditionValues = new HashSet<>();

    @Field("feature_type")
    private Set<String> featureTypes = new HashSet<>();

    @Field("keyword")
    private Set<String> keywords = new HashSet<>();

    @Field("gene")
    private Set<String> geneNames = new HashSet<>();

    @Field("go")
    private Set<String> goTerms = new HashSet<>();

    @Field("protein_name")
    private Set<String> proteinNames = new HashSet<>();

    @Field("organism")
    private Set<String> organismNames = new HashSet<>();

    @Field("taxonomy")
    private Set<String> taxonomyNames = new HashSet<>();

    @Field("superkingdom")
    public Set<String> superKingdoms = new HashSet<>();

    @Field("cc_*")
    private Map<String, Set<String>> commentTypeValues = new HashMap<>();

    @Field("ec")
    private Set<String> ecNumbers = new HashSet<>();

    @Field("family")
    private Set<String> families = new HashSet<>();

    @Field("unirule_obj")
    private ByteBuffer uniRuleObj;

    @Override
    public String getDocumentId() {
        return uniRuleId;
    }
}
