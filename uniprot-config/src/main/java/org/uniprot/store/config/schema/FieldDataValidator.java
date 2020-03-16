package org.uniprot.store.config.schema;

import static java.util.Collections.emptyList;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.model.Field;

public class FieldDataValidator<T extends Field> {
    public void validateContent(List<T> fieldItems) {
        Set<String> ids = extractIds(fieldItems);
        validateParentExists(fieldItems, ids);
        validateSeqNumbers(fieldItems);
        validateChildren(fieldItems);
        validateUniqueIDs(fieldItems);
    }

    protected Set<String> extractIds(List<T> fieldItems) {
        return fieldItems.stream().map(Field::getId).collect(Collectors.toSet());
    }

    void validateParentExists(List<T> fieldItems, Set<String> ids) {
        fieldItems.stream()
                .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                .forEach(
                        fieldItem -> {
                            if (!ids.contains(fieldItem.getParentId())) {
                                throw new SchemaValidationException(
                                        "Field Item doesn't exist for parentId '"
                                                + fieldItem.getParentId()
                                                + "'");
                            }
                        });
    }

    void validateSeqNumbers(List<T> fieldItems) {
        List<Integer> seqNumbers = extractSeqNumbers(fieldItems);
        validateNaturalNumbers(seqNumbers, "seqNumber");
    }

    void validateChildren(List<T> fieldItems) {
        Map<String, List<T>> parentChildrenMap =
                fieldItems.stream()
                        .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                        .collect(Collectors.groupingBy(Field::getParentId));

        parentChildrenMap.forEach(this::validateChildNumbers);

        List<T> parentNodes = extractParentNodes(fieldItems);
        everyParentHasAChild(parentNodes, parentChildrenMap);
    }

    /**
     * This should not be derived from nodes that have a non-null parentId, but rather, based on
     * their item type.
     *
     * @param fieldItems a list of all the fields known
     * @return a list of nodes that are parents
     */
    protected List<T> extractParentNodes(List<T> fieldItems) {
        return emptyList();
    }

    private void everyParentHasAChild(List<T> parentNodes, Map<String, List<T>> parentChildrenMap) {
        List<String> parentsWithNoChildren = new ArrayList<>();
        parentNodes.forEach(
                parent -> {
                    if (!parentChildrenMap.containsKey(parent.getId())) {
                        parentsWithNoChildren.add(parent.getId());
                    }
                });
        if (!parentsWithNoChildren.isEmpty()) {
            throw new SchemaValidationException(
                    "Parent nodes must have children, but these do not: "
                            + String.join(", ", parentsWithNoChildren));
        }
    }

    private void validateUniqueIDs(List<T> fieldItems) {
        Map<String, Integer> duplicateCounts = new HashMap<>();
        fieldItems.stream()
                .map(Field::getId)
                .forEach(
                        id ->
                                duplicateCounts.compute(
                                        id,
                                        (existingId, count) -> (count == null) ? 1 : count + 1));

        StringBuilder duplicatesBuilder = new StringBuilder();
        duplicateCounts.forEach(
                (id, count) -> {
                    if (count > 1) {
                        duplicatesBuilder
                                .append("\t\"")
                                .append(id)
                                .append("\" was used ")
                                .append(count)
                                .append(" times\n");
                    }
                });
        String duplicates = duplicatesBuilder.toString();
        if (duplicates.length() > 0) {
            throw new SchemaValidationException(
                    "IDs used contain duplicates, but must be unique: \n" + duplicates);
        }
    }

    private List<Integer> extractSeqNumbers(List<T> fieldItems) {
        return fieldItems.stream()
                .filter(field -> field.getSeqNumber() != null)
                .map(Field::getSeqNumber)
                .collect(Collectors.toList());
    }

    private void validateChildNumbers(String parentId, List<T> children) {
        List<Integer> childNumbers =
                children.stream().map(Field::getChildNumber).collect(Collectors.toList());
        String message = "childNumber for parentId '" + parentId + "'";
        validateNaturalNumbers(childNumbers, message);
    }

    private void validateNaturalNumbers(List<Integer> numbers, String message) {
        // check numbers are natural number including 0
        int inputSize = numbers.size();
        BitSet visitedSet = new BitSet(inputSize);

        for (Integer number : numbers) {
            if (number == null) {
                throw new SchemaValidationException(message + " number is null.");
            }
            if (number >= inputSize) {
                throw new SchemaValidationException(
                        message + " " + number + " is bigger than available number.");
            }
            if (number < 0) {
                throw new SchemaValidationException(message + " " + number + " is less than zero.");
            }
            if (visitedSet.get(number)) {
                throw new SchemaValidationException(message + " " + number + " is already used.");
            }
            visitedSet.set(number);
        }
    }
}
