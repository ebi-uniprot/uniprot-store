package org.uniprot.store.config.service.factory;

import org.uniprot.store.config.common.UniProtDataType;
import org.uniprot.store.config.service.ConfigFieldService;
import org.uniprot.store.config.service.impl.UniProtConfigFieldServiceImpl;

public class ConfigFieldServiceFactory {
    public static ConfigFieldService getConfigFieldService(UniProtDataType dataType){
        if(UniProtDataType.UNIPROTKB == dataType) {
            return UniProtConfigFieldServiceImpl.getInstance();
        }
        return null;
    }
}
