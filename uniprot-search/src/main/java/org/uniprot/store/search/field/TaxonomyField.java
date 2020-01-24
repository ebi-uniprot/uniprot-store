package org.uniprot.store.search.field;

import java.util.Arrays;

public interface TaxonomyField {

    enum ResultFields implements ReturnField {
        id("Taxon"),
        taxonId("Taxon Id", "taxonId", true), // always present in json response
        parent("Parent", "parentId"),
        mnemonic("Mnemonic", "mnemonic"),
        scientific_name("Scientific name", "scientificName"),
        common_name("Common name", "commonName"),
        synonym("Synonym", "synonyms"),
        other_names("Other Names", "otherNames"),
        rank("Rank", "rank"),
        reviewed("Reviewed", "reviewed"),
        lineage("Lineage", "lineage"),
        strain("Strain", "strains"),
        host("Virus hosts", "hosts"),
        link("Link", "links"),
        statistics("Statistics", "statistics"),
        active_internal("Active", "active"),
        inactiveReason_internal("Inactive Reason", "inactiveReason");

        private String label;
        private String javaFieldName;
        private boolean isMandatoryJsonField;

        ResultFields(String label) {
            this(label, null);
        }

        ResultFields(String label, String javaFieldName) {
            this(label, javaFieldName, false);
        }

        ResultFields(String label, String javaFieldName, boolean isMandatoryJsonField) {
            this.label = label;
            this.javaFieldName = javaFieldName;
            this.isMandatoryJsonField = isMandatoryJsonField;
        }

        public String getLabel() {
            return this.label;
        }

        @Override
        public boolean hasReturnField(String fieldName) {
            return Arrays.stream(ResultFields.values())
                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
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
