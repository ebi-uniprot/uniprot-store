package org.uniprot.store.job.common.store;

import java.util.Collection;

/**
 * Created 24/07/19
 *
 * @author Edd
 */
@FunctionalInterface
public interface Store<S> {
    void save(Collection<? extends S> items);
}
