package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.stream.Collectors;

public interface DiseaseField {

    enum ResultFields implements ReturnField {
        id("Name", "id", true),
        accession("DiseaseEntry ID", "accession", true),
        acronym("Mnemonic", "acronym", true),
        definition("Description", "definition", true),
        alternative_names("Alternative Names", "alternativeNames"),
        cross_references("Cross Reference", "crossReferences"),
        keywords("Keywords", "keywords"),
        reviewed_protein_count("Reviewed Protein Count", "reviewedProteinCount"),
        unreviewed_protein_count("Unreviewed Protein Count", "unreviewedProteinCount");

        private String label;
        private String javaFieldName;
        private boolean isDefault;

        ResultFields(String label, String javaFieldName) {
            this(label, javaFieldName, false);
        }

        ResultFields(String label, String javaFieldName, boolean isDefault) {
            this.label = label;
            this.javaFieldName = javaFieldName;
            this.isDefault = isDefault;
        }

        public String getLabel() {
            return this.label;
        }

        public boolean isDefault() {
            return this.isDefault;
        }

        public static String getDefaultFields() {
            return Arrays.stream(ResultFields.values())
                    .filter(f -> f.isDefault)
                    .map(f -> f.name())
                    .collect(Collectors.joining(","));
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
    }
}
