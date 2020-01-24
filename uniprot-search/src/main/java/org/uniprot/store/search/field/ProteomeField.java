package org.uniprot.store.search.field;

public interface ProteomeField {
    enum Return {
        upid,
        proteome_stored;
    }

    // boost organism_name^2.0
    // boost organism_id^2.0

    enum ResultFields implements ReturnField {
        id,
        description,
        taxonomy,
        modified,
        proteomeType,
        redundantTo,
        strain,
        isolate,
        dbXReferences,
        components,
        references,
        redundantProteomes,
        panproteome,
        annotationScore,
        superkingdom,
        geneCount,
        taxonLineage,
        canonicalProteins,
        sourceDb;

        @Override
        public boolean hasReturnField(String fieldName) {
            return false;
        }

        @Override
        public String getJavaFieldName() {
            return this.name();
        }
    }
}
