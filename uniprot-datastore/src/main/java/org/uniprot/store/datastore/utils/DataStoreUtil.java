package org.uniprot.store.datastore.utils;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
public class DataStoreUtil {

    // ---------------------- Source Data Access beans and helpers ----------------------
    /**
     * Checks if the given object is a proxy, and unwraps it if it is.
     *
     * @param bean The object to check
     * @return The unwrapped object that was proxied, else the object
     * @throws Exception any exception caused during unwrapping
     */
    public static Object unwrapProxy(Object bean) throws Exception {
        if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
            Advised advised = (Advised) bean;
            bean = advised.getTargetSource().getTarget();
        }
        return bean;
    }
}
