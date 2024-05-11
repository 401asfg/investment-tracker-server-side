/**
 * An investment into a vehicle
 *
 * @param dateTime The date and time at which this investment was made
 * @param principal The amount invested
 * @param vehicle The vehicle invested into
 * @param id The unique identifier of this past price
 */
data class Investment(val dateTime: DateTime, val principal: Float, val vehicle: Vehicle, var id: Int? = null)
