package org.uniprot.store.search.field;

/**
 * @author jluo
 * @date: 19 Aug 2019
 */
public interface UniRefField {
    enum Return {
        id
    }

    enum ResultFields implements ReturnField {
        id("id"),
        name("name"),
        common_taxon("commonTaxon"),
        common_taxonid("commonTaxonId"),
        count("memberCount"),
        organism_id("members"),
        organism("members"),
        identity("entryType"),
        length("representativeMember"),
        sequence("representativeMember"),
        member("members"),
        created("updated"),
        go("goTerms");

        private String javaFieldName;
        private boolean isMandatoryJsonField;

        ResultFields() {
            this(null);
        }

        ResultFields(String javaFieldName) {
            this(javaFieldName, false);
        }

        ResultFields(String javaFieldName, boolean isMandatoryJsonField) {
            this.javaFieldName = javaFieldName;
            this.isMandatoryJsonField = isMandatoryJsonField;
        }

        @Override
        public boolean hasReturnField(String fieldName) {
            return false;
        }

        @Override
        public String getJavaFieldName() {
            return this.javaFieldName;
        }

        @Override
        public boolean isMandatoryJsonField() {
            return this.isMandatoryJsonField;
        }
    }
}
