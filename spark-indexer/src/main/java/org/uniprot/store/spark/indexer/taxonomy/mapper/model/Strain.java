package org.uniprot.store.spark.indexer.taxonomy.mapper.model;

import java.io.Serializable;

import org.uniprot.core.util.Utils;

import lombok.Getter;

@Getter
public class Strain implements Serializable {
    private static final long serialVersionUID = 6343238023359679585L;

    private final long id;
    private final StrainNameClass nameClass;
    private final String name;

    public Strain(long id, StrainNameClass nameClass, String name) {
        this.id = id;
        this.nameClass = nameClass;
        this.name = name;
    }

    public enum StrainNameClass {
        scientific_name,
        synonym;

        public static StrainNameClass fromQuery(String value) {
            if (Utils.notNullNotEmpty(value)) {
                if (value.equals("scientific name")) {
                    return StrainNameClass.scientific_name;
                } else if (value.equals("synonym")) {
                    return StrainNameClass.synonym;
                }
            }
            return null;
        }
    }
}
