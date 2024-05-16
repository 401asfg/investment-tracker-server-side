package com.michael.investmenttrackerserverside.model

// TODO: test

/**
 * A UTC (+00:00) date and time
 */
data class DateTime(val year: Int, val month: Int, val day: Int, val hour: Int, val minute: Int) {
    /**
     * Initializes this class
     *
     * @param timestamp A timestamp formatted as: YYYY-MM-DD hh:mm:00
     */
    constructor(timestamp: String) : this(
        timestamp.subSequence(0, 4).toString().toInt(),
        timestamp.subSequence(5, 7).toString().toInt(),
        timestamp.subSequence(8, 10).toString().toInt(),
        timestamp.subSequence(11, 13).toString().toInt(),
        timestamp.subSequence(14, 16).toString().toInt()
    )

    /**
     * @return The timestamp representation of this date time
     */
    fun toTimestamp(): String = "$year-$month-$day $hour:$minute:00"
}

/**
 * A smallest unit of time to which all the points in time in an interval are rounded
 */
enum class TimeGranularity {
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE
}

/**
 * An interval of time that includes the given from and to date times, and only includes times that round to the given
 * granularity
 *
 * @param from The earliest date time that is included in this interval
 * @param to The latest date time that is included in this interval
 * @param granularity The unit of time that a date time must be rounded to, to be included in this interval
 */
data class Interval(val from: DateTime, val to: DateTime, val granularity: TimeGranularity = TimeGranularity.MINUTE) {
    /**
     * @param dateTime The date time to check if its considered included in this interval
     * @return True if the given date time is included in this interval, otherwise false
     */
    fun included(dateTime: DateTime): Boolean = false   // TODO: implement stub
}
