package org.uniprot.store.search.field.validator;

/**
 * This class is responsible to have search field value validation Predicates
 *
 * @author lgonzales
 */
public class FieldValueValidator {

    public static final String ACCESSION_REGEX =
            "([O,P,Q][0-9][A-Z|0-9]{3}[0-9]|[A-N,R-Z]([0-9][A-Z][A-Z|0-9]{2}){1,2}[0-9])(-\\d+)*";
    public static final String PROTEOME_ID_REX = "UP[0-9]{9}";
    public static final String UNIPARC_UPI_REX = "UPI[\\w]{10}";
    public static final String UNIREF_CLUSTER_ID_REX = "(UniRef100|UniRef90|UniRef50)[\\w]+";
    /**
     * This method is responsible to validate any accession value
     *
     * @param value uniprot accession value
     * @return true if it is a valid value, otherwise returns false.
     */
    public static boolean isAccessionValid(String value) {
        boolean result = false;
        if (value != null) {
            result = value.toUpperCase().matches(ACCESSION_REGEX);
        }
        return result;
    }

    public static boolean isUpidValid(String value) {
        boolean result = false;
        if (value != null) {
            result = value.toUpperCase().matches(PROTEOME_ID_REX);
        }
        return result;
    }

    public static boolean isUpiValid(String value) {
        boolean result = false;
        if (value != null) {
            result = value.toUpperCase().matches(UNIPARC_UPI_REX);
        }
        return result;
    }

    public static boolean isUniRefIdValid(String value) {
        boolean result = false;
        if (value != null) {
            result = value.matches(UNIREF_CLUSTER_ID_REX);
        }
        return result;
    }

    /**
     * This method is responsible to validate any true|false boolean value
     *
     * @param value field value
     * @return true if it is a valid value, otherwise returns false.
     */
    public static boolean isBooleanValue(String value) {
        boolean result = false;
        String booleanRegex = "^true|false$";
        if (value != null) {
            result = value.matches(booleanRegex);
        }
        return result;
    }

    /**
     * This method is responsible to validate any numeric value
     *
     * @param value field value
     * @return true if it is a valid value, otherwise returns false.
     */
    public static boolean isNumberValue(String value) {
        boolean result = false;
        String numberRegex = "^[0-9]+$";
        if (value != null) {
            result = value.matches(numberRegex);
        }
        return result;
    }

    /**
     * This method is responsible to validate proteome id value
     *
     * @param value field value
     * @return true if it is a valid value, otherwise returns false.
     */
    public static boolean isProteomeIdValue(String value) {
        boolean result = false;
        String numberRegex = "^UP[0-9]{9}$";
        if (value != null) {
            result = value.toUpperCase().matches(numberRegex);
        }
        return result;
    }

    /**
     * This method is responsible to validate proteome id value
     *
     * @param value field value
     * @return true if it is a valid value, otherwise returns false.
     */
    public static boolean isKeywordIdValue(String value) {
        boolean result = false;
        String numberRegex = "^KW-[0-9]{4}$";
        if (value != null) {
            result = value.toUpperCase().matches(numberRegex);
        }
        return result;
    }

    public static boolean isDiseaseIdValue(String value) {
        boolean result = false;
        String numberRegex = "^DI-[0-9]{5}$";
        if (value != null) {
            result = value.toUpperCase().matches(numberRegex);
        }
        return result;
    }

    public static boolean isCrossRefIdValue(String value) {
        boolean result = false;
        String numberRegex = "^DB-[0-9]{4}$";
        if (value != null) {
            result = value.toUpperCase().matches(numberRegex);
        }
        return result;
    }

    /**
     * This method is responsible to validate subcellular location id value
     *
     * @param value field value
     * @return true if it is a valid value, otherwise returns false.
     */
    public static boolean isSubcellularLocationIdValue(String value) {
        boolean result = false;
        String numberRegex = "^SL-[0-9]{4}$";
        if (value != null) {
            result = value.toUpperCase().matches(numberRegex);
        }
        return result;
    }
}
