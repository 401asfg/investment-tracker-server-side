/**
 * A portfolio of investments
 *
 * @param investments The investments made in this portfolio
 * @param usdToBaseCurrencyRateVehicle The vehicle whose value at a given time is the exchange rate at that time from
 * USD to this portfolios base currency
 * @param id The unique identifier of this past price
 */
data class Portfolio(val investments: List<Investment>, val usdToBaseCurrencyRateVehicle: Vehicle, var id: Int? = null)
