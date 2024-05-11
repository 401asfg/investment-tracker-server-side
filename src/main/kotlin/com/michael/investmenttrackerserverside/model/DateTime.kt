/**
 * A UTC (+00:00) date and time
 */
data class DateTime(val year: Int, val month: Int, val day: Int, val hour: Int, val minute: Int) {
    /**
     * @return The timestamp representation of this date time
     */
    fun toTimestamp(): String = "$year-$month-$day $hour:$minute:00"
}
