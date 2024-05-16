import com.michael.investmenttrackerserverside.model.DateTime

/**
 * An investment into a vehicle
 *
 * @param dateTime The date and time at which this investment was made
 * @param principal The amount invested
 * @param vehicle The vehicle invested into
 * @param portfolioId The unique identifier of the portfolio this investment belongs to
 * @param id The unique identifier of this past price
 */
data class Investment(
    val dateTime: DateTime,
    val principal: Float,
    val vehicle: Vehicle,
    val portfolioId: Int,
    var id: Int? = null
)
