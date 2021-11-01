package org.uniprot.store.search.document.arba;

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

/**
 * @author lgonzales
 * @since 16/07/2021
 */
@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ArbaDocument implements Document {

    @Field("rule_id")
    private String ruleId;

    @Field("condition_value")
    private Set<String> conditionValues = new HashSet<>();

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
    private Set<String> superKingdoms = new HashSet<>();

    @Field("cc_*")
    private Map<String, Set<String>> commentTypeValues = new HashMap<>();

    @Field("ec")
    private Set<String> ecNumbers = new HashSet<>();

    @Field("family")
    private Set<String> families = new HashSet<>();

    @Field("rule_obj")
    private ByteBuffer ruleObj;

    @Override
    public String getDocumentId() {
        return ruleId;
    }
}
