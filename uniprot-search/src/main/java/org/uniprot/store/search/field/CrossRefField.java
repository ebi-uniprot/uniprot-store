package org.uniprot.store.search.field;

import java.util.Arrays;

public interface CrossRefField {

    enum ResultFields implements ReturnField {
        name("Name", "name", true),
        id("Id", "id", true),
        abbrev("Abbrev", "abbrev", true),
        pub_med_id("Pub Med Id", "pubMedId", true),
        doi_id("DOI Id", "doiId", true),
        link_type("Link Type", "linkType", true),
        server("Server", "server", true),
        db_url("DB URL", "dbUrl", true),
        category("Category", "category", true),
        reviewed_protein_count("Reviewed Protein Count", "reviewedProteinCount", true),
        unreviewed_protein_count("Unreviewed Protein Count", "unreviewedProteinCount", true);

        private String label;
        private String fieldName;
        private boolean isDefault;

        ResultFields(String label, String fieldName, boolean isDefault) {
            this.label = label;
            this.fieldName = fieldName;
            this.isDefault = isDefault;
        }

        public String getLabel() {
            return this.label;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        public boolean isDefault() {
            return this.isDefault;
        }

        @Override
        public boolean hasReturnField(String fieldName) {
            return Arrays.stream(ResultFields.values())
                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
        }

        @Override
        public String getJavaFieldName() {
            return this.fieldName;
        }
    }
}
