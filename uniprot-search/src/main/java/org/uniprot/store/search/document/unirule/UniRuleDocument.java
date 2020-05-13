package org.uniprot.store.search.document.unirule;

import java.nio.ByteBuffer;
import java.util.*;

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
    private List<String> conditionValues = new ArrayList<>();

    @Field("feature_type")
    private List<String> featureTypes;

    @Field("keyword")
    private List<String> keywords = new ArrayList<>();

    @Field("gene")
    private List<String> geneNames = new ArrayList<>();

    @Field("go")
    private List<String> goTerms = new ArrayList<>();

    @Field("protein_name")
    private List<String> proteinNames = new ArrayList<>();

    @Field("organism")
    private List<String> organismNames = new ArrayList<>();

    @Field("taxonomy")
    private List<String> taxonomyNames = new ArrayList<>();

    @Field("cc_*")
    private Map<String, Collection<String>> commentTypeValues = new HashMap<>();

    @Field("content")
    private List<String> content = new ArrayList<>();

    @Field("unirule_obj")
    private ByteBuffer uniRuleObj;

    @Override
    public String getDocumentId() {
        return uniRuleId;
    }
}
