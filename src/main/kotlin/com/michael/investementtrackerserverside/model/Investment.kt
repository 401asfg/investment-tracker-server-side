/**
 * An investment into a vehicle
 *
 * @param id The unique identifier of this past price
 * @param dateTime The date and time at which this investment was made
 * @param principal The amount invested
 * @param vehicle The vehicle invested into
 */
data class Investment(val id: Int, val dateTime: DateTime, val principal: Float, val vehicle: Vehicle)
