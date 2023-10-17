package org.uniprot.store.config.returnfield.config.impl;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtDatabaseAttribute;
import org.uniprot.core.cv.xdb.UniProtDatabaseCategory;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.core.util.Utils;
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

    private static final long serialVersionUID = 1163013837966442698L;
    private UniProtDatabaseTypes databaseTypes;
    private Map<String, UniProtDatabaseCategory> databaseCategoryMap;
    public static final Map<String, String> DBNAMEPATH =
            Map.of("dbsnp", "features[?(@.type=='Natural variant')]");

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

    @Override
    public ReturnField getReturnFieldByName(String fieldName) {
        ReturnField result = null;
        if (!fieldName.endsWith("_full")) {
            result = super.getReturnFieldByName(fieldName);
        } else {
            String internalFieldName = fieldName.substring(0, fieldName.indexOf("_full"));
            try {
                ReturnField found = super.getReturnFieldByName(internalFieldName);
                if (found.getIsMultiValueCrossReference()) {
                    result = found.toBuilder().name(fieldName).build();
                } else {
                    throw new IllegalStateException(
                            String.format(
                                    "%s is not a multi value cross-reference and it does not support %s field name",
                                    internalFieldName, fieldName));
                }
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }
        }
        return result;
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
        returnField.setName("xref_" + databaseNameLowercase);
        returnField.setIsMultiValueCrossReference(
                isMultiValueCrossReference(uniProtDatabaseDetail));
        returnField.setLabel(dbName);

        if (isDBNotInUniProtKBCrossReferences(databaseNameLowercase)) {
            returnField.addPath(getPathByDBName(databaseNameLowercase));
        } else {
            returnField.addPath("uniProtKBCrossReferences[?(@.database=='" + dbName + "')]");
        }
        returnField.setId(parent.getId() + "/" + databaseNameLowercase);
        returnField.setIncludeInSwagger(true);
        return returnField;
    }

    private Boolean isMultiValueCrossReference(UniProtDatabaseDetail uniProtDatabaseDetail) {
        boolean result = false;
        List<UniProtDatabaseAttribute> attributes = uniProtDatabaseDetail.getAttributes();
        if (Utils.notNullNotEmpty(attributes)) {
            result = attributes.size() > 1 || !attributes.get(0).equals(UniProtDatabaseDetail.DEFAULT_ATTRIBUTE);
        }
        return result;
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

    private boolean isDBNotInUniProtKBCrossReferences(String databaseNameLowercase) {
        return DBNAMEPATH.containsKey(databaseNameLowercase);
    }

    private String getPathByDBName(String databaseNameLowercase) {
        return DBNAMEPATH.get(databaseNameLowercase);
    }
}
