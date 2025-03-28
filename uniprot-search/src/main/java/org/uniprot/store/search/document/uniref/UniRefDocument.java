package org.uniprot.store.search.document.uniref;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.*;

/**
 * @author jluo
 * @date: 13 Aug 2019
 */
@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class UniRefDocument implements Document {

    private static final long serialVersionUID = -1168863804678801054L;

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

    @Field("date_modified")
    private Date dateModified;

    @Field("date_modified_sort")
    private Date dateModifiedSort;

    @Singular
    @Field("uniprotkb")
    private List<String> uniprotIds;

    @Singular
    @Field("cluster")
    private Set<String> clusters;

    @Field("organism_sort")
    public String organismSort;

    @Singular
    @Field("uniparc")
    private List<String> uniParcIds;

    @Singular
    @Field("taxonomy_name")
    private List<String> organismTaxons;

    @Singular
    @Field("taxonomy_id")
    private List<Integer> taxLineageIds;

    @Override
    public String getDocumentId() {
        return id;
    }
}
