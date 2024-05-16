package com.michael.investmenttrackerserverside

import Investment
import PastPrice
import com.michael.investmenttrackerserverside.model.Interval
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

        /**
         * Builds a statement for inserting data into the database
         *
         * @param connection The connection to the database that is used to prepare the statement
         * @param table The table to insert into
         * @param columnNames The names of the columns to fill out
         * @return An SQL statement that inserts the given columnValues into each of the given columnNames, in the
         * given table
         */
        private fun buildInsertStatement(
            connection: Connection,
            table: String,
            columnNames: Set<String>
        ): PreparedStatement {
            // FIXME: make sure this isn't vulnerable to sql injections
            val columnNamesString = columnNames.joinToString(SET_SEPARATOR)
            val valuesPlaceholdersString = columnNames.joinToString(SET_SEPARATOR) { VALUE_PLACEHOLDER }
            val sql = "INSERT INTO $table ($columnNamesString) VALUES ($valuesPlaceholdersString);"
            return connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)
        }
    }

    /**
     * Inserts the given portfolio and its investments into the database, without inserting the usd to base currency
     * rate vehicle
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

        val id = insert(PORTFOLIO_TABLE, setOf("usd_to_base_currency_rate_vehicle_id")) { ps ->
            ps.setInt(1, usdToBaseCurrencyRateVehicleId)
        }

        portfolio.id = id
        insert(portfolio.investments)
    }

    /**
     * Inserts the given investments into the database, without inserting its vehicle
     * Assigns each investment the unique id of the database row that represents it
     *
     * @param investments The investments to insert
     * @throws DataAccessException If the database was unable to perform the insertion
     * @throws MissingVehicleException If any of the investments' vehicle ids are null
     */
    fun insert(investments: List<Investment>) {
        val ids = insert(
            INVESTMENT_TABLE,
            setOf("date_time", "principal", "vehicle_id", "portfolio_id"),
            investments.size
        ) { ps: PreparedStatement, i: Int ->
            val investment = investments[i]

            ps.setString(1, investment.dateTime.toTimestamp())
            ps.setFloat(2, investment.principal)

            val vehicleId = investment.vehicle.id
            if (vehicleId === null) throw MissingVehicleException("An investment's vehicle id is null")

            ps.setInt(3, vehicleId)
            ps.setInt(4, investment.portfolioId)
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
            val vehicle = vehicles[i]

            ps.setString(1, vehicle.symbol)
            ps.setString(2, vehicle.name)
        }

        val pastPrices = mutableListOf<PastPrice>()

        vehicles.forEachIndexed { i, vehicle ->
            vehicle.id = ids[i]
            pastPrices.addAll(vehicle.pastPrices)
        }

        insert(pastPrices)
    }

    /**
     * Inserts the past prices into the database
     * Assigns each past price the unique id of the database row that represents it
     *
     * @param pastPrices The past price to insert
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    fun insert(pastPrices: List<PastPrice>) {
        val ids = insert(
            PAST_PRICE_TABLE,
            setOf("date_time", "price", "is_closing", "vehicle_id"),
            pastPrices.size
        ) { ps: PreparedStatement, i: Int ->
            val pastPrice = pastPrices[i]

            ps.setString(1, pastPrice.dateTime.toTimestamp())
            ps.setFloat(2, pastPrice.price)
            ps.setBoolean(3, pastPrice.isClosing)
            ps.setInt(4, pastPrice.vehicleId)
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
        val keyHolder = GeneratedKeyHolder()

        jdbcTemplate.batchUpdate(
            { con -> buildInsertStatement(con, table, columnNames) },
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
     * @param setValues Specifies the value the assigned to each of the columns, for one row
     * @return The unique id of the inserted row
     * @throws DataAccessException If the database was unable to perform the insertion
     */
    private fun insert(table: String, columnNames: Set<String>, setValues: (PreparedStatement) -> Unit): Int {
        val keyHolder = GeneratedKeyHolder()

        jdbcTemplate.update(
            { con ->
                val ps = buildInsertStatement(con, table, columnNames)
                setValues(ps)
                ps
            },
            keyHolder
        )

        return keyHolder.key as Int
    }

    /**
     * TODO: write documentation
     */
    fun queryPortfolio(id: Int, interval: Interval): Portfolio
        = Portfolio(listOf(), Vehicle("", "", listOf()), id)    // TODO: implement stub

    /**
     * TODO: write documentation
     */
    private fun queryInvestments(
        portfolioId: Int,
        interval: Interval
    ): Set<Investment> = setOf()    // TODO: implement stub

    /**
     * TODO: write documentation
     */
    fun queryVehicles(query: String): Set<Vehicle> = setOf()    // TODO: implement stub

    /**
     * TODO: write documentation
     */
    fun queryVehicle(id: Int, interval: Interval): Vehicle
        = Vehicle("", "", listOf()) // TODO: implement stub

    /**
     * TODO: write documentation
     */
    private fun queryVehicles(portfolioId: Int, interval: Interval): Set<Vehicle>
        = setOf()   // TODO: implement stub

    /**
     * TODO: write documentation
     */
    private fun queryPastPrices(
        portfolioId: Int,
        interval: Interval
    ): Set<PastPrice> = setOf() // TODO: implement stub
}