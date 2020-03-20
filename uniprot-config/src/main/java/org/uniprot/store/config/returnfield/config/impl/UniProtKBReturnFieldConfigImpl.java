package org.uniprot.store.config.returnfield.config.impl;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtDatabaseCategory;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.config.returnfield.config.AbstractReturnFieldConfig;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;

/**
 * Loads a valid JSON definition of return field definitions for UniProtKB, and adds dynamically
 * database information from {@link UniProtDatabaseTypes}.
 *
 * <p>The requirement for dynamic loading of database fields from {@link UniProtDatabaseTypes}, for
 * a specific {@link UniProtDatabaseCategory}, is that the database category is defined as one of
 * the groups in the static JSON definition. The reason for not dynamically loading the categories
 * of databases is that the static definition in JSON allows a clear ordering to be defined that is
 * easily changeable.
 *
 * <p>Created 13/03/20
 *
 * @author Edd
 */
public class UniProtKBReturnFieldConfigImpl extends AbstractReturnFieldConfig {
    private UniProtDatabaseTypes databaseTypes;
    private Map<String, UniProtDatabaseCategory> databaseCategoryMap;

    public UniProtKBReturnFieldConfigImpl(String configFile) {
        super(configFile);
    }

    @Override
    protected Collection<ReturnField> dynamicallyLoadFields() {
        init();
        return super.allFields.stream()
                .filter(ReturnField::getIsDatabaseGroup)
                .map(this::loadAndAddDBGroupFields)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    ReturnField databaseToReturnField(
            UniProtDatabaseDetail uniProtDatabaseDetail,
            ReturnField parent,
            AtomicInteger childCounter) {
        ReturnField returnField = new ReturnField();
        returnField.setParentId(parent.getId());
        returnField.setChildNumber(childCounter.getAndIncrement());
        returnField.setItemType(ReturnFieldItemType.SINGLE);
        String dbName = uniProtDatabaseDetail.getName();
        String databaseNameLowercase = dbName.toLowerCase();
        returnField.setName("dr_" + databaseNameLowercase);
        returnField.setLabel(dbName);
        returnField.addPath("uniProtCrossReferences[?(@.database=='" + dbName + "')]");
        returnField.setId(parent.getId() + "/" + databaseNameLowercase);
        return returnField;
    }

    UniProtDatabaseCategory getDatabaseCategory(String groupName) {
        String key;
        if (databaseCategoryMap.containsKey(groupName + " databases")) {
            key = groupName + " databases";
        } else if (databaseCategoryMap.containsKey(groupName)) {
            key = groupName;
        } else {
            throw new IllegalArgumentException("Unknown database category: " + groupName);
        }
        return databaseCategoryMap.get(key);
    }

    private void init() {
        this.databaseTypes = UniProtDatabaseTypes.INSTANCE;
        databaseCategoryMap = new HashMap<>();
        Arrays.stream(UniProtDatabaseCategory.values())
                .forEach(category -> databaseCategoryMap.put(category.getDisplayName(), category));
    }

    private List<ReturnField> loadAndAddDBGroupFields(ReturnField databaseGroup) {
        UniProtDatabaseCategory category = getDatabaseCategory(databaseGroup.getGroupName());
        List<UniProtDatabaseDetail> databases = databaseTypes.getDBTypesByCategory(category);
        // ensure the databases are ordered by their name
        databases.sort(Comparator.comparing(UniProtDatabaseDetail::getDisplayName));
        AtomicInteger childCounter = new AtomicInteger();
        return databases.stream()
                .filter(database -> !database.isImplicit())
                .map(database -> databaseToReturnField(database, databaseGroup, childCounter))
                .collect(Collectors.toList());
    }
}
