/**
 * A portfolio of investments
 *
 * @param id The unique identifier of this past price
 * @param investments The investments made in this portfolio
 * @param usdToBaseCurrencyRateVehicle The vehicle whoes value at a given time is the exchange rate at that time from
 * USD to this portfolios base currency
 */
data class Portfolio(val id: Int, val investments: Set<Investment>, val usdToBaseCurrencyRateVehicle: Vehicle)
