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
