package uk.ac.ebi.uniprot.datastore.voldemort;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a client of the Voldemort key / value data-store.
 * @param <T> the entity type being stored in Voldemort.
 */
public interface VoldemortClient<T> {
    String getStoreName();

    Optional<T> getEntry(String id);

    List<T> getEntries(Iterable<String> ids);

    Map<String, T> getEntryMap(Iterable<String> ids);

    void saveEntry(T entry);
}