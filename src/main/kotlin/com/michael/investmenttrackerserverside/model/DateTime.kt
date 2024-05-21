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
 * An interval of time that includes the given from and to date times, and only includes times that round to the given
 * granularity
 *
 * @param from The earliest date time that is included in this interval
 * @param to The latest date time that is included in this interval
 * @param timeGranularity The unit of time that a date time must be rounded to, to be included in this interval
 * @throws IllegalArgumentException If the given granularity isn't a valid unit of time
 */
data class Interval(val from: DateTime, val to: DateTime, val timeGranularity: String = YEAR_GRANULARITY) {
    companion object {
        const val YEAR_GRANULARITY = "year"
        const val MONTH_GRANULARITY = "month"
        const val DAY_GRANULARITY = "day"
        const val HOUR_GRANULARITY = "hour"
        const val MINUTE_GRANULARITY = "minute"
    }

    init {
        if (timeGranularity != YEAR_GRANULARITY
            && timeGranularity != MONTH_GRANULARITY
            && timeGranularity != DAY_GRANULARITY
            && timeGranularity != HOUR_GRANULARITY
            && timeGranularity != MINUTE_GRANULARITY)
            throw IllegalArgumentException("Interval received invalid granularity: $timeGranularity")
    }
}
