package org.uniprot.store.search.field;

public interface UniProtField {
    // All the fields for uniprotkb entry object. For filtering using jsonresponse projector.please
    // see class UniProtResultFields
    enum ResultFields implements ReturnField {
        entryType("entryType", true),
        primaryAccession("primaryAccession", true),
        secondary_accession("secondaryAccessions"),
        uniProtId("uniProtId", true),
        entryAudit("entryAudit", true),
        annotationScore("annotationScore", true),
        organism("organism"),
        organism_host("organismHosts"),
        protein_existence("proteinExistence"),
        protein_name("proteinDescription"),
        gene("genes"),
        comment("comments"),
        feature("features"),
        organelle("geneLocations"),
        keyword("keywords"),
        reference("references"),
        crossReference("uniProtCrossReferences"),
        sequence("sequence"),
        internalSection_internal("internalSection"),
        inactiveReason_internal("inactiveReason", true),
        lineage("lineages"),
        length,
        mass;

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

    enum Return {
        accession
    }
}
