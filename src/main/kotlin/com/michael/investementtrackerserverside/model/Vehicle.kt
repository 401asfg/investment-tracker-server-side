/**
 * A vehicle that's price changes over time and can be invested in
 *
 * @param id The unique identifier of this past price
 * @param symbol The symbol of this vehicle
 * @param name The name of this vehicle
 * @param pastPrices The prices of this vehicle on prior dates
 * @param numQueries The number of times this vehicle has been queried in the database
 */
data class Vehicle(
    val id: Int,
    val symbol: String,
    val name: String,
    val pastPrices: Set<PastPrice>,
    val numQueries: Int
)
