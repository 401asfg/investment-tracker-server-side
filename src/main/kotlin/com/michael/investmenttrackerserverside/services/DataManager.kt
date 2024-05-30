package com.michael.investmenttrackerserverside.services

import Investment
import PastPrice
import Portfolio
import Vehicle
import com.michael.investmenttrackerserverside.model.Interval

// FIXME: create interface for Database and DataManager that forces both to implement same query, insert, a delete methods

/**
 * Used to store, show, and destroy data
 *
 * @param database The data to store and destroy data within; shows data from database if it exists there
 */
class DataManager(val database: Database) {
    /**
     * TODO: write documentation
     */
    fun store(portfolio: Portfolio) { database.insert(portfolio) }

    /**
     * TODO: write documentation
     */
    fun store(investment: Investment) { database.insert(investment) }

    /**
     * TODO: write documentation
     */
    fun store(vehicles: List<Vehicle>) { database.insert(vehicles) }

    /**
     * TODO: write documentation
     */
    fun store(pastPrices: List<PastPrice>) { database.insert(pastPrices) }

    /**
     * TODO: write documentation
     */
    fun showPortfolio(id: Int): Portfolio = Portfolio(listOf(), Vehicle("", "", listOf()))  // TODO: implement stub

    /**
     * TODO: write documentation
     */
    fun showVehicles(query: String): Set<Vehicle> = setOf() // TODO: implement stub

    /**
     * TODO: write documentation
     */
    fun showVehicle(symbol: String): Vehicle = Vehicle("", "", listOf())    // TODO: implement stub

    /**
     * TODO: write documentation
     */
    fun showPastPrices(portfolioId: Int, interval: Interval): Map<Int, Set<PastPrice>> = mapOf()    // TODO: implement stub

    /**
     * TODO: write documentation
     */
    fun destroyPortfolio(id: Int) { database.deletePortfolio(id) }

    /**
     * TODO: write documentation
     */
    fun destroyInvestment(id: Int) { database.deleteInvestment(id) }

    /**
     * TODO: write documentation
     */
    fun destroyVehicle(id: Int) { database.deleteVehicle(id) }

    /**
     * TODO: write documentation
     */
    fun destroyPastPrices(vehicleId: Int, interval: Interval) { database.deletePastPrices(vehicleId, interval) }
}