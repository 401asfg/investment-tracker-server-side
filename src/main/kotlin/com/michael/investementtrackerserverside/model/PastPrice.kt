/**
 * The price of a vehicle at a point in time
 *
 * @param id The unique identifier of this past price
 * @param dateTime The date and time that the vehicle had this given price
 * @param price The price of the vehicle at the given dateTime
 * @param isClosing Whether or not this past price was the closing price on its date
 */
data class PastPrice(val id: Int, val dateTime: DateTime, val price: Float, val isClosing: Boolean)
