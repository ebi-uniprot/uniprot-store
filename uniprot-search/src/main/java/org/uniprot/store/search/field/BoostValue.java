package org.uniprot.store.search.field;

import lombok.Getter;

/**
 * This class represents a boosted value of a field, and is used in {@link SearchField} definitions.
 * For example, an instance with value="term1", boost=2.1f Created 26/06/19
 *
 * @author Edd
 */
@Getter
public class BoostValue {
    private String value;
    private Float boost;

    private BoostValue(String value, Float boost) {
        this.value = value;
        this.boost = boost;
    }

    public static BoostValue boostValue(Float boost) {
        return new BoostValue(null, boost);
    }

    public static BoostValue boostValue(String value, Float boost) {
        return new BoostValue(value, boost);
    }
}
