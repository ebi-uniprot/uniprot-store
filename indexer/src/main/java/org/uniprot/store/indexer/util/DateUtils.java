package org.uniprot.store.indexer.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Contains several utility methods that help with date manipulation
 */
public final class DateUtils {
    private DateUtils() {
    }

    /**
     * Converts the given date into a UTC date, without the time zone
     *
     * @param date the date to convert
     * @return converted date with timezone discrepancy added
     */
    public static Date convertDateToUTCDate(Date date) {
        LocalDate localDate = convertDateToLocalDate(date, ZoneId.systemDefault());
        return convertLocalDateToDate(localDate, ZoneOffset.UTC);
    }

    /**
     * Converts a LocalDate into the old Date format, using the system default timezone
     *
     * @param localDate the local date to convert
     * @return the converted date
     */
    public static Date convertLocalDateToDate(LocalDate localDate) {
        return convertLocalDateToDate(localDate, ZoneId.systemDefault());
    }

    /**
     * Converts a LocalDate into the old Date format. The method allows you to specify the timezone to convert with.
     *
     * @param localDate the Local date to convert from
     * @param zoneId    the timezone offset to use
     * @return the converted date
     */
    private static Date convertLocalDateToDate(LocalDate localDate, ZoneId zoneId) {
        Instant instant = localDate.atStartOfDay(zoneId).toInstant();
        return Date.from(instant);
    }

    /**
     * Converts an old {@link Date} instance into a {@link LocalDate}. Uses the system default timezone
     *
     * @param date the date to convert
     * @return the converted date
     */
    public static LocalDate convertDateToLocalDate(Date date) {
        return convertDateToLocalDate(date, ZoneId.systemDefault());
    }

    /**
     * Converts an old {@link Date} instance into a {@link LocalDate}. The method allows you to specify the timezone to
     * convert with.
     *
     * @param date   the date to convert
     * @param zoneId the timezone to convert with
     * @return the converted date
     */
    private static LocalDate convertDateToLocalDate(Date date, ZoneId zoneId) {
        return LocalDate.from(date.toInstant().atZone(zoneId));
    }
}