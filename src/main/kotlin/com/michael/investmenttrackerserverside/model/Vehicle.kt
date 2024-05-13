/**
 * A vehicle that's price changes over time and can be invested in
 *
 * @param symbol The symbol of this vehicle
 * @param name The name of this vehicle
 * @param pastPrices The prices of this vehicle on prior dates
 * @param id The unique identifier of this past price
 */
data class Vehicle(
    val symbol: String,
    val name: String,
    val pastPrices: List<PastPrice>,
    var id: Int? = null
)
