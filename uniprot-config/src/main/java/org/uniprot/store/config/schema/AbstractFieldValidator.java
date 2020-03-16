package org.uniprot.store.config.schema;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.model.Field;

public abstract class AbstractFieldValidator<T extends Field> {
    public abstract void validateContent(List<T> fieldItems);

    public Set<String> extractIds(List<T> fieldItems) {
        return fieldItems.stream().map(Field::getId).collect(Collectors.toSet());
    }

    public void validateParentExists(List<T> fieldItems, Set<String> ids) {
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

    public void validateSeqNumbers(List<T> fieldItems) {
        List<Integer> seqNumbers = extractSeqNumbers(fieldItems);
        validateNaturalNumbers(seqNumbers, "seqNumber");
    }

    public void validateChildNumbers(List<T> fieldItems) {
        Map<String, List<T>> parentChildrenMap =
                fieldItems.stream()
                        .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                        .collect(Collectors.groupingBy(Field::getParentId));

        parentChildrenMap.forEach(this::validateChildNumbers);
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

    // TODO: 15/03/20 validate ids are unique
}
