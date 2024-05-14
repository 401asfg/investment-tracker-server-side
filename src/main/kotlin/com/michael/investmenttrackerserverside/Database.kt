package com.michael.investmenttrackerserverside

import DateTime
import Investment
import PastPrice
import Portfolio
import Vehicle
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.*
import org.springframework.jdbc.support.GeneratedKeyHolder
import java.sql.*
import javax.sql.DataSource

// TODO: test
// FIXME: more explicit errors
// FIXME: refactor duplicate code in insert methods

class MissingVehicleException(msg: String? = null) : Exception(msg)

/**
 * A database that can have models saved and loaded to it
 *
 * @param datasource The connection to the database
 */
class Database(val datasource: DataSource) {
    private val jdbcTemplate = JdbcTemplate(datasource)

    companion object {
        const val PAST_PRICE_TABLE = "past_prices"
        const val VEHICLE_TABLE = "vehicles"
        const val INVESTMENT_TABLE = "investments"
        const val PORTFOLIO_TABLE = "portfolios"

        private const val ID_COLUMN = "id"
        private const val SET_SEPARATOR = ", "
        private const val VALUE_PLACEHOLDER = "?"
    }

    // FIXME: change inserts to use transactions

    /**
     * Inserts the given portfolio and its investments into the database
     * Assigns the portfolio the unique id of the database row that represents it
     *
     * @param portfolio The portfolio to insert
     * @throws DataAccessException If the database was unable to perform the insertion
     * @throws MissingVehicleException If the portfolio's usd to base currency rate vehicle id is null
     */
    fun insert(portfolio: Portfolio) {
        val usdToBaseCurrencyRateVehicleId = portfolio.usdToBaseCurrencyRateVehicle.id

        if (usdToBaseCurrencyRateVehicleId === null)
            throw MissingVehicleException("Portfolio's usd to base currency rate vehicle id is null")

        val id = insert(
            PORTFOLIO_TABLE,
            setOf("usd_to_base_currency_rate_vehicle_id"),
            setOf(usdToBaseCurrencyRateVehicleId.toString())
        )

        portfolio.id = id
        insert(portfolio.investments, portfolio.id!!)
    }

    /**
     * Inserts the given investments into the database
     * Assigns each investment the unique id of the database row that represents it
     *
     * @param investments The investments to insert
     * @param portfolioId The id of the portfolio each investment is associated with
     * @throws DataAccessException If the database was unable to perform the insertion
     * @throws MissingVehicleException If any of the investments' vehicle ids are null
     */
    fun insert(investments: List<Investment>, portfolioId: Int) {
        val ids = insert(
            INVESTMENT_TABLE,
            setOf("date_time", "principal", "vehicle_id", "portfolio_id"),
            investments.size
        ) { ps: PreparedStatement, i: Int ->
            ps.setString(1, investments[i].dateTime.toTimestamp())
            ps.setFloat(2, investments[i].principal)

            val vehicleId = investments[i].vehicle.id

            if (vehicleId === null)
                throw MissingVehicleException("An investment's vehicle id is null")

            ps.setInt(3, vehicleId)
            ps.setInt(4, portfolioId)
        }

        investments.forEachIndexed { i, investment -> investment.id = ids[i] }
    }

    /**
     * Inserts the given vehicles and their past prices into the database
     * Assigns each vehicle the unique id of the database row that represents it
     *
     * @param vehicles The vehicles to insert
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    fun insert(vehicles: List<Vehicle>) {
        val ids = insert(
            VEHICLE_TABLE,
            setOf("symbol", "name"),
            vehicles.size
        ) { ps: PreparedStatement, i: Int ->
            ps.setString(1, vehicles[i].symbol)
            ps.setString(2, vehicles[i].name)
        }

        val vehiclePastPricesPairs = mutableListOf<Pair<Int, List<PastPrice>>>()

        vehicles.forEachIndexed { i, vehicle ->
            vehicle.id = ids[i]
            vehiclePastPricesPairs.add(Pair(vehicle.id!!, vehicle.pastPrices))
        }

        insert(vehiclePastPricesPairs)
    }

    /**
     * Inserts the past prices of each vehicle into the database
     * Assigns each past price the unique id of the database row that represents it
     *
     * @param vehiclePastPricesPairs The past price sets that are each associated with a different vehicle id
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    fun insert(vehiclePastPricesPairs: List<Pair<Int, List<PastPrice>>>, vehicleId: Int) {
        val batchSize = vehiclePastPricesPairs.fold(0) { batchSize, vehiclePastPricesPair ->
            batchSize + vehiclePastPricesPair.second.size
        }

        val ids = insert(
            PAST_PRICE_TABLE,
            setOf("date_time", "price", "is_closing", "vehicle_id"),
            batchSize
        ) { ps: PreparedStatement, i: Int ->
            ps.setString(1, pastPrices[i].dateTime.toTimestamp())
            ps.setFloat(2, pastPrices[i].price)
            ps.setBoolean(3, pastPrices[i].isClosing)
            ps.setInt(4, vehicleId)
        }

        pastPrices.forEachIndexed { i, pastPrice -> pastPrice.id = ids[i] }
    }

    /**
     * Inserts into the given database's table
     *
     * @param table The table to insert into
     * @param columnNames The names of the columns to fill out
     * @param batchSize The number of rows to be inserted
     * @param setValues Specifies the value the assigned to each of the columns, for each inserted row
     * @return The unique ids of the inserted rows
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    private fun insert(
        table: String,
        columnNames: Set<String>,
        batchSize: Int,
        setValues: (PreparedStatement, Int) -> Unit
    ): List<Int> {
        val columnValues = columnNames.map { "?" }
        val sql = buildInsertSQL(table, columnNames, columnValues.toSet())
        val keyHolder = GeneratedKeyHolder()

        jdbcTemplate.batchUpdate(
            { con -> con.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS) },
            object: BatchPreparedStatementSetter {
                override fun setValues(ps: PreparedStatement, i: Int) { setValues(ps, i) }
                override fun getBatchSize(): Int = batchSize
            },
            keyHolder
        )

        return keyHolder.keyList.map { keyMap -> keyMap[ID_COLUMN] as Int }
    }

    /**
     * Inserts into the given database's table
     *
     * @param table The table to insert into
     * @param columnNames The names of the columns to fill out
     * @param columnValues The values to insert into each of the named columns; Must be the same size as columnNames
     * @return The unique id of the inserted row
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    private fun insert(table: String, columnNames: Set<String>, columnValues: Set<String>): Int {
        val sql = buildInsertSQL(table, columnNames, columnValues)
        val keyHolder = GeneratedKeyHolder()

        jdbcTemplate.update(
            { con -> con.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS) },
            keyHolder
        )

        return keyHolder.key as Int
    }

    /**
     * @param table The table to insert into
     * @param columnNames The names of the columns to fill out
     * @param columnValues The values to insert into each of the named columns; Must be the same size as columnNames
     * @return An SQL statement that inserts the given columnValues into each of the given columnNames, in the given
     * table
     */
    private fun buildInsertSQL(table: String, columnNames: Set<String>, columnValues: Set<String>): String {
        val columnNamesString = columnNames.joinToString(SET_SEPARATOR)
        val columnValuesString = columnValues.joinToString(SET_SEPARATOR)
        return "INSERT INTO $table ($columnNamesString) VALUES ($columnValuesString);"
    }

    // TODO: implement the rest of the queries
    /*
    portfolioId -> entire tree structure, using 4 join queries
    q -> vehicles without past prices, using 1 query
    vehicleId, fromDate, toDate, granularity -> past prices that meet specs, using 1 query

    queryPortfolio(portfolioId: Int, fromDate: DateTime, toDate: DateTime, granularity: Granularity): Portfolio
    private queryInvestments(portfolioId: Int, fromDate: DateTime, toDate: DateTime, granularity: Granularity): List<Investment>
    private queryVehicles(portfolioId: Int, fromDate: DateTime, toDate: DateTime, granularity: Granularity): List<Vehicles>
    private queryPastPrices(portfolioId: Int): List<PastPrice>

    queryVehicles(query: String): List<Vehicle> (NO PAST PRICES)
    queryPastPrices(vehicleId: Int, fromDate: DateTime, toDate: DateTime, granularity: Granularity): List<PastPrice>
     */

    /**
     * Queries vehicles from the database
     *
     * @param ids The ids of each of the produced vehicles
     * @return The vehicles in the database that each correspond to one of the given ids
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    fun queryVehicles(ids: List<Int>): List<Vehicle> {
        val idsString = ids.joinToString(SET_SEPARATOR)

        return query(VEHICLE_TABLE, "'id' IN ($idsString)") { rs, _ ->
            val id = rs.getInt(ID_COLUMN)
            Vehicle(
                rs.getString("symbol"),
                rs.getString("name"),
                queryPastPrices(id),
                id
            )
        }
    }

    /**
     * Queries past prices from the database
     *
     * @param vehicleId The vehicle id that all produced past prices must have
     * @return All the past prices in the database that have the given vehicleId
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    fun queryPastPrices(vehicleId: Int): List<PastPrice>
        = query(PAST_PRICE_TABLE, "'vehicle_id' = $vehicleId") { rs, _ ->
            PastPrice(
                DateTime(rs.getString("date_time")),
                rs.getFloat("price"),
                rs.getBoolean("is_closing"),
                rs.getInt("id")
            )
        }

    /**
     * Queries rows from the given table, filtering with the given whereClause
     *
     * @param table The table to query from
     * @param whereClause The where clause of the query, filters what rows are queries
     * @param mapRow Maps the values of a row queried to an object that is produced
     * @return A series of objects that contain the values of the rows queried
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    private fun <T> query(table: String, whereClause: String, mapRow: (ResultSet, Int) -> T): List<T> {
        val querySQL = "SELECT * FROM $table WHERE $whereClause;"
        return jdbcTemplate.query(querySQL, mapRow)
    }
}