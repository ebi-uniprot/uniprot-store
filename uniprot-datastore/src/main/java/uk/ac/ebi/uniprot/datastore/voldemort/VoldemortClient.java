package uk.ac.ebi.uniprot.datastore.voldemort;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface VoldemortClient<T> {
    String getStoreName();

    Optional<T> getEntry(String id);

    List<T> getEntries(Iterable<String> ids);

    Map<String, T> getEntryMap(Iterable<String> ids);

    void saveEntry(T entry);
}
