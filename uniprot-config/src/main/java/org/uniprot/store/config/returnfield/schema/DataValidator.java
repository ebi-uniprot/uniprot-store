package org.uniprot.store.config.returnfield.schema;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.schema.SchemaValidationException;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DataValidator {

    public static void validateContent(
        List<ReturnField> fieldItems, Set<String> ids) {
        validateParentExists(fieldItems, ids);
        validateSeqNumbers(fieldItems);
        validateChildNumbers(fieldItems);
    }

    public static void validateParentExists(
        List<ReturnField> fieldItems, Set<String> ids) {
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

    public static void validateSeqNumbers(List<ReturnField> fieldItems) {
        List<Integer> seqNumbers = extractSeqNumbers(fieldItems);
        validateNaturalNumbers(seqNumbers, "seqNumber");
    }

    private static List<Integer> extractSeqNumbers(List<ReturnField> fieldItems) {
        return fieldItems.stream()
                .filter(field -> field.getSeqNumber() != null)
                .map(ReturnField::getSeqNumber)
                .collect(Collectors.toList());
    }

    public static void validateChildNumbers(List<ReturnField> fieldItems) {
        Map<String, List<ReturnField>> parentChildrenMap =
                fieldItems.stream()
                        .filter(fi -> StringUtils.isNotBlank(fi.getParentId()))
                        .collect(Collectors.groupingBy(ReturnField::getParentId));

        parentChildrenMap
            .forEach(DataValidator::validateChildNumbers);
    }

    private static void validateChildNumbers(String parentId, List<ReturnField> children) {
        List<Integer> childNumbers =
                children.stream().map(ReturnField::getChildNumber).collect(Collectors.toList());
        String message = "childNumber for parentId '" + parentId + "'";
        validateNaturalNumbers(childNumbers, message);
    }

    // TODO: 04/03/2020 extract to common class, e.g., in schema package (not the one in resultfield package)
    private static void validateNaturalNumbers(List<Integer> numbers, String message) {
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
                throw new SchemaValidationException(
                        message + " " + number + " is less than zero.");
            }
            if (visitedSet.get(number)) {
                throw new SchemaValidationException(
                        message + " " + number + " is already used.");
            }
            visitedSet.set(number);
        }
    }

    private DataValidator() {}
}
