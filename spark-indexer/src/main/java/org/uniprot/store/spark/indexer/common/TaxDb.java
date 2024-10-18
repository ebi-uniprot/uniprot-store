package org.uniprot.store.spark.indexer.common;

import lombok.Getter;

@Getter
public enum TaxDb {
    READ("read"),
    FLY("fly");

    private final String name;

    TaxDb(String name) {
        this.name = name;
    }

    public static TaxDb forName(String name) {
        for (TaxDb taxDb : values()) {
            if (taxDb.name.equals(name)) {
                return taxDb;
            }
        }
        throw new IllegalArgumentException(
                "Wrong tax db name %s, Can be only either read or fly.".formatted(name));
    }
}
