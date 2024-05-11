package com.michael.investmenttrackerserverside

import DateTime
import Investment
import PastPrice
import Portfolio
import Vehicle
import java.sql.*

// TODO: test
// FIXME: more explicit errors
// FIXME: refactor duplicate code in insert methods

/**
 * A database that can have models saved and loaded to it
 *
 * @param url The url at which the database can be connected to
 * @param user The username used to connect to the database
 * @param password The password used to connect to the database
 */
class Database(val url: String, val user: String, val password: String) {
    private val connection = DriverManager.getConnection(url, user, password)

    companion object {
        const val PAST_PRICE_TABLE = "past_prices"
        const val VEHICLE_TABLE = "vehicles"
        const val INVESTMENT_TABLE = "investments"
        const val PORTFOLIO_TABLE = "portfolios"

        const val ID_COLUMN = "id"

        /**
         * @param items The item set to build a string representation of
         * @param itemToString A function that converts an item into a string
         * @return A string representation of the given item set
         */
        private fun <T> buildSetString(items: Set<T>, itemToString: (T) -> String): String
            = items.fold("") { itemsString, item ->
                "${if (itemsString == "") "" else ", "}${itemToString(item)}"
            }

        /**
         * @param values The set of strings to build a value set string representation of
         * @return A string representation of the given set of strings, enclosed by parentheses
         */
        private fun buildValuesSetString(values: Set<String>): String = "(${buildSetString(values) { item -> item }})"
    }

    /**
     * Inserts the given portfolio to the database
     *
     * @param portfolio The portfolio to insert
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     */
    fun insert(portfolio: Portfolio) {
        val id = portfolio.id.toString()
        val usdToBaseCurrencyRateVehicleId = portfolio.usdToBaseCurrencyRateVehicle.id.toString()

        val values = setOf(id, usdToBaseCurrencyRateVehicleId)
        portfolio.id = executePortfolioInsert(values)

        val investments = portfolio.investments
        insert(investments, portfolio.id!!)
    }

    /**
     * Inserts the given investments to the database
     *
     * @param investments The investments to insert
     * @param portfolioId The id of the portfolio each investment is associated with
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     */
    fun insert(investments: Set<Investment>, portfolioId: Int) {
        val valueSets = mutableSetOf<Set<String>>()
        val portfolioIdString = portfolioId.toString()

        investments.forEach { investment ->
            val dateTimestamp = investment.dateTime.toTimestamp()
            val principal = investment.principal.toString()
            val vehicleId = investment.vehicle.id.toString()

            valueSets.add(setOf(dateTimestamp, principal, vehicleId, portfolioIdString))
        }

        val ids = executeInsert(INVESTMENT_TABLE, valueSets)
        investments.forEachIndexed { i, investment -> investment.id = ids[i] }
    }

    /**
     * Inserts the given vehicles to the database
     *
     * @param vehicles The vehicles to insert
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     */
    fun insert(vehicles: Set<Vehicle>) {
        val valueSets = mutableSetOf<Set<String>>()

        vehicles.forEach { vehicle ->
            val symbol = vehicle.symbol
            val name = vehicle.name

            valueSets.add(setOf(symbol, name))
        }

        val ids = executeInsert(VEHICLE_TABLE, valueSets)

        vehicles.forEachIndexed { i, vehicle ->
            vehicle.id = ids[i]
            insert(vehicle.pastPrices, vehicle.id!!)
        }
    }

    /**
     * Inserts the given past prices to the database
     *
     * @param pastPrices The past prices to insert
     * @param vehicleId The id of the vehicle each past price is associated with
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     * @throws IllegalArgumentException If pastPrices and vehicleIds don't have the same size
     */
    fun insert(pastPrices: Set<PastPrice>, vehicleId: Int) {
        val valueSets = mutableSetOf<Set<String>>()
        val vehicleIdString = vehicleId.toString()

        pastPrices.forEach { pastPrice ->
            val dateTimestamp = pastPrice.dateTime.toTimestamp()
            val price = pastPrice.price.toString()
            val isClosing = pastPrice.isClosing.toString()

            valueSets.add(setOf(dateTimestamp, price, isClosing, vehicleIdString))
        }

        val ids = executeInsert(PAST_PRICE_TABLE, valueSets)
        pastPrices.forEachIndexed { i, pastPrice -> pastPrice.id = ids[i] }
    }

    /**
     * Queries the portfolio with the given id
     *
     * @param id The id of the portfolio to obtain
     * @return The portfolio that corresponds to the given id
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     * @throws IllegalArgumentException If the given id doesn't correspond to any row in the portfolio table
     * @throws SQLIntegrityConstraintViolationException If any foreign key in the portfolio or its references are
     * invalid
     */
    fun queryPortfolio(id: Int): Portfolio {
        val resultSet = executeQuery(PORTFOLIO_TABLE, id)
        if (!resultSet.next()) throw IllegalArgumentException("Query failed to obtain any results")

        val usdToBaseCurrencyRateVehicleId = resultSet.getInt("usd_to_base_currency_rate_vehicle_id")
        val investments = queryInvestments(id)
        val usdToBaseCurrencyRateVehicle = queryVehicle(usdToBaseCurrencyRateVehicleId)

        return Portfolio(investments, usdToBaseCurrencyRateVehicle, id)
    }

    /**
     * Queries the investments with the given portfolioId
     *
     * @param portfolioId The id of the portfolio that contains these investments
     * @return The investments that have the given portfolioId
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     * @throws SQLIntegrityConstraintViolationException If any foreign key in the portfolio or its references are
     * invalid
     */
    private fun queryInvestments(portfolioId: Int): Set<Investment> {
        val resultSet = executeQuery(INVESTMENT_TABLE, "'portfolio_id' = $portfolioId")
        val investments = mutableSetOf<Investment>()

        while (resultSet.next()) {
            val dateTime = DateTime(resultSet.getString("date_time"))
            val principal = resultSet.getFloat("principal")
            val vehicle = queryVehicle(resultSet.getInt("vehicle_id"))
            val id = resultSet.getInt(ID_COLUMN)

            val investment = Investment(dateTime, principal, vehicle, id)
            investments.add(investment)
        }

        return investments
    }

    /**
     * TODO: write documentation
     */
    private fun queryVehicle(id: Int): Vehicle {
        // TODO: implement
        return Vehicle("", "", setOf())
    }

    /**
     * Executes an insert statement with the given value sets against the database
     *
     * @param table The name of the table to insert into
     * @param valueSets The sets of values to insert
     * @return The ids of the rows inserted
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     */
    private fun executeInsert(table: String, valueSets: Set<Set<String>>): List<Int> {
        val valueSetsString = buildSetString(valueSets, Database::buildValuesSetString)
        return executeInsert(table, valueSetsString)
    }

    /**
     * Executes an insert statement with the given values against the portfolio table of the database
     *
     * @param values The values to insert
     * @return The id of the row inserted
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     * @throws IllegalArgumentException If inserting the given values doesn't affect exactly one row
     */
    private fun executePortfolioInsert(values: Set<String>): Int {
        val valuesString = buildValuesSetString(values)
        val ids = executeInsert(PORTFOLIO_TABLE, valuesString)

        if (ids.size != 1) throw IllegalArgumentException("Insert of single set of values affected ${ids.size} rows")
        return ids[0]
    }

    /**
     * Executes an insert statement with the given string of value sets against the database
     *
     * @param table The name of the table to insert into
     * @param valuesString The string of values to insert
     * @return The ids of the rows inserted
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database insert couldn't be performed in the allotted time
     */
    private fun executeInsert(table: String, valuesString: String): List<Int>
        = executeWrite("INSERT INTO $table VALUES $valuesString;")

    /**
     * Executes the given statement against the database
     *
     * @param statement The SQL write statement to execute
     * @return The ids of the rows written
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database write couldn't be performed in the allotted time
     */
    private fun executeWrite(statement: String): List<Int> {
        val instruction = connection.prepareStatement(statement, PreparedStatement.RETURN_GENERATED_KEYS)
        val numRowsAffected = instruction.executeUpdate()

        if (numRowsAffected == 0) throw SQLException("No rows were written to on executing a database write statement")

        val generatedKeys = instruction.generatedKeys
        val ids = mutableListOf<Int>()

        while (generatedKeys.next()) ids.add(generatedKeys.getInt(ID_COLUMN))
        return ids
    }

    /**
     * Queries the given table for the row with the given id
     *
     * @param table The name of the table to query
     * @param id The id of the row to obtain
     * @return The fields of the row in the given table with the given id
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database read couldn't be performed in the allotted time
     */
    private fun executeQuery(table: String, id: Int): ResultSet = executeQuery(table, "'id' = $id")

    /**
     * Queries the given table for the rows with the given ids
     *
     * @param table The name of the table to query
     * @param ids The ids of the rows to obtain
     * @return The fields of the rows in the given table with the given ids
     * @throws SQLException If inserting into the database fails
     * @throws SQLTimeoutException If the database read couldn't be performed in the allotted time
     */
    private fun executeQuery(table: String, ids: Set<Int>): ResultSet {
        val idsString = buildSetString(ids) { id -> id.toString() }
        return executeQuery(table, "'id' IN ($idsString)")
    }

    /**
     * Queries the given table based on the given whereClause
     *
     * @param table The name of the table to query
     * @param whereClause The clause stating the conditions regarding what is queried
     * @return The result of the query
     * @throws SQLException If querying the database fails
     * @throws SQLTimeoutException If the database read couldn't be performed in the allotted time
     */
    private fun executeQuery(table: String, whereClause: String): ResultSet
        = executeRead("SELECT * FROM $table WHERE $whereClause;")

    /**
     * Executes the given statement against the database
     *
     * @param statement The SQL read statement to execute
     * @return The result of the query
     * @throws SQLException If reading from the database fails
     * @throws SQLTimeoutException If the database read couldn't be performed in the allotted time
     */
    private fun executeRead(statement: String): ResultSet {
        val query = connection.prepareStatement(statement)
        return query.executeQuery()
    }
}