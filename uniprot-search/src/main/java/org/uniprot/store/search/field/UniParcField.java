package org.uniprot.store.search.field;

/**
 * @author jluo
 * @date: 18 Jun 2019
 */
public interface UniParcField {
    enum Return {
        upi,
        entry_stored;
    }

    enum ResultFields implements ReturnField {
        uniParcId,
        databaseCrossReferences,
        sequence,
        uniprotExclusionReason,
        sequenceFeatures,
        taxonomies;

        @Override
        public boolean hasReturnField(String fieldName) {
            return false;
        }

        @Override
        public String getJavaFieldName() {
            return this.name();
        }

        @Override
        public boolean isMandatoryJsonField() {
            return false;
        }
    }
}
