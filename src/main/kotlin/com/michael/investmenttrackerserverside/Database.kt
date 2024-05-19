package com.michael.investmenttrackerserverside

import Investment
import PastPrice
import com.michael.investmenttrackerserverside.model.Interval
import Portfolio
import Vehicle
import com.michael.investmenttrackerserverside.model.DateTime
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.*
import org.springframework.jdbc.support.GeneratedKeyHolder
import java.sql.*
import javax.sql.DataSource

// TODO: test
// FIXME: more explicit errors
// FIXME: refactor duplicate code in insert methods

/**
 * Thrown when a vehicle that should be in the database is not in the database
 */
class MissingVehicleException(msg: String? = null) : Exception(msg)

/**
 * Thrown when a portfolio that should be in the database is not in the database
 */
class MissingPortfolioException(msg: String? = null) : Exception(msg)

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
            val columnNamesString = columnNames.joinToString(", ")
            val valuesPlaceholdersString = columnNames.joinToString(", ") { "?" }
            val sql = "INSERT INTO $table ($columnNamesString) VALUES ($valuesPlaceholdersString);"
            return connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)
        }

        /**
         * @param sql The sql statement to prepare
         * @return A function that prepares the given sql statement
         */
        private fun prepareStatementClosure(sql: String): (Connection) -> PreparedStatement
            = { con -> con.prepareStatement(sql) }

        /**
         * Sets the portfolio id in the given preparedStatement with the value of the given portfolioId
         *
         * @param preparedStatement The statement to set the portfolio id of
         * @param portfolioId The value to set the portfolio id in the statement to
         * @throws SQLException If the statement contains no portfolio id
         */
        private fun setPortfolioId(preparedStatement: PreparedStatement, portfolioId: Int) {
            preparedStatement.setInt(1, portfolioId)
        }

        /**
         * @param portfolioId The portfolio id to set in a prepared statement
         * @return A function that sets the portfolio id in a prepared statement
         */
        private fun setPortfolioIdClosure(portfolioId: Int): (PreparedStatement) -> Unit
            = { ps -> setPortfolioId(ps, portfolioId) }

        /**
         * @param portfolioId The portfolio id to set in a prepared statement
         * @param interval The interval to set in a prepared statement
         * @return A function that sets the portfolio id in a prepared statement
         */
        private fun setPastPricesClosure(portfolioId: Int, interval: Interval): (PreparedStatement) -> Unit
            = { ps ->
                val fromDate = interval.from.toTimestamp()
                val toDate = interval.to.toTimestamp()
                val requiredTimestampEnd = interval.requiredTimestampEnd

                setPortfolioId(ps, portfolioId)
                ps.setString(2, fromDate)
                ps.setString(3, toDate)
                ps.setString(4, requiredTimestampEnd)
            }

        // FIXME: make sure build fns should take in maps and not just what they need

        /**
         * @param resultSet The data to build a vehicle from
         * @param vehicle The investment's vehicle
         * @return An investment from the given resultSet data
         * @throws SQLException If the given resultSet didn't contain some of the data needed to build an investment
         */
        private fun buildInvestment(resultSet: ResultSet, vehicle: Vehicle): Investment {
            val dateTime = DateTime(resultSet.getString("date_time"))
            val principal = resultSet.getFloat("principal")
            val portfolioId = resultSet.getInt("portfolio_id")
            val id = resultSet.getInt("id")

            return Investment(dateTime, principal, vehicle, portfolioId, id)
        }

        /**
         * @param resultSet The data to build a vehicle from
         * @param pastPrices The past prices that have the vehicle's id
         * @return A vehicle from the given resultSet data and the given vehiclesPastPrices
         * @throws SQLException If the given resultSet didn't contain some of the data needed to build a vehicle
         */
        private fun buildVehicle(resultSet: ResultSet, pastPrices: List<PastPrice>): Vehicle {
            val symbol = resultSet.getString("symbol")
            val name = resultSet.getString("name")
            val id = resultSet.getInt("id")

            return Vehicle(symbol, name, pastPrices, id)
        }

        /**
         * @param resultSet The data to build a past price from
         * @return A past price from given resultSet data
         * @throws SQLException If the given resultSet didn't contain some of the data needed to build a past price
         */
        private fun buildPastPrice(resultSet: ResultSet): PastPrice {
            val dateTime = DateTime(resultSet.getString("date_time"))
            val price = resultSet.getFloat("price")
            val isClosing = resultSet.getBoolean("is_closing")
            val vehicleId = resultSet.getInt("is_vehicle_id")
            val id = resultSet.getInt("id")

            return PastPrice(dateTime, price, isClosing, vehicleId, id)
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

            ps.setString(1, "DATETIME(${investment.dateTime.toTimestamp()})")
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

            ps.setString(1, "DATETIME(${pastPrice.dateTime.toTimestamp()})")
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

        return keyHolder.keyList.map { keyMap -> keyMap["id"] as Int }
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
     * @param id The unique identifier of the portfolio
     * @param interval Only past prices, associated with the vehicles of the queried portfolio, that are included in
     * this interval are queried
     * @return The portfolio associated with the given id; only includes the past prices that are included in the given
     * interval
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create the portfolio
     * @throws MissingVehicleException If all the queried portfolio didn't have all its associated vehicles in the
     * database
     * @throws MissingPortfolioException If there is no portfolio associated with the given id
     */
    fun queryPortfolio(id: Int, interval: Interval): Portfolio {
        val sql = """
            SELECT id
            FROM $PORTFOLIO_TABLE
            WHERE id = ?
        """.trimIndent()

        if (query(sql, null, setPortfolioIdClosure(id)) { rs -> rs.getInt("id") } === null)
            throw MissingPortfolioException("The portfolio with the id: $id could not be found")

        val investments = queryPortfolioInvestments(id, interval)
        val usdToBaseCurrencyRateVehicle = queryPortfolioUsdToBaseCurrencyRateVehicle(id, interval)
        return Portfolio(investments, usdToBaseCurrencyRateVehicle, id)
    }

    /**
     * @param portfolioId A unique identifier of a portfolio; Only investments associated with this portfolio are
     * queried
     * @param interval Only past prices, associated with the vehicles of the queried investments, that are included in
     * this interval are queried
     * @return All the investments associated with the given portfolioId; only includes the past prices that are
     * included in the given interval
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create investments
     * @throws MissingVehicleException If all the queried investments don't each have a corresponding vehicle
     */
    private fun queryPortfolioInvestments(portfolioId: Int, interval: Interval): List<Investment> {
        val sql = """
            SELECT
                i.id AS id,
                i.date_time AS date_time,
                i.principal AS principal,
                i.vehicle_id AS vehicle_id,
                i.portfolio_id AS portfolio_id
            FROM $INVESTMENT_TABLE as i
            INNER JOIN $PORTFOLIO_TABLE as p
                ON i.portfolio_id = p.id
            WHERE p.id = ?
            """.trimIndent()

        val investmentsVehicles = queryPortfolioInvestmentsVehicles(portfolioId, interval)

        return query(sql, listOf(), setPortfolioIdClosure(portfolioId)) { rs ->
            val investments = mutableListOf<Investment>()

            while (rs.next()) {
                val vehicleId = rs.getInt("vehicle_id")

                val vehicle = investmentsVehicles[vehicleId]
                    ?: throw MissingVehicleException("Query for investment couldn't find a corresponding vehicle")

                val investment = buildInvestment(rs, vehicle)
                investments.add(investment)
            }

            investments
        }
    }

    /**
     * @param portfolioId A unique identifier of a portfolio; only the usd to base currency rate vehicle is queried
     * @param interval Only past prices that are included in this interval, are included in their vehicles
     * @return The usd to base currency rate vehicle in the portfolio with the given portfolioId; only includes the
     * past prices that are included in the given interval
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create a vehicle
     * @throws MissingVehicleException If the query results didn't contain exactly one vehicle
     */
    private fun queryPortfolioUsdToBaseCurrencyRateVehicle(portfolioId: Int, interval: Interval): Vehicle {
        val sql = """
            SELECT
                v.id AS id,
                v.symbol AS symbol,
                v.name AS name
            FROM $VEHICLE_TABLE AS v
            INNER JOIN $PORTFOLIO_TABLE as p
                ON v.id = p.usd_to_base_currency_rate_vehicle_id
            WHERE p.id = ?
            """.trimIndent()

        val pastPrices = queryPortfolioUsdToBaseCurrencyRateVehiclePastPrices(portfolioId, interval)

        // FIXME: move single and multiple result setter bodies into their own static, generic methods

        return query(sql, null, setPortfolioIdClosure(portfolioId)) { rs ->
            if (!rs.next())
                throw MissingVehicleException(
                    "Query for a portfolio's usd to base currency rate vehicle didn't produce any results"
                )

            val vehicle = buildVehicle(rs, pastPrices)

            if (rs.next())
                throw MissingVehicleException(
                    "Query for a portfolio's usd to base currency rate vehicle produced more than one result"
                )

            vehicle
        }!!
    }

    /**
     * @param portfolioId A unique identifier of a portfolio; only vehicles associated with this portfolio, through
     * their investments are queried
     * @param interval Only past prices that are included in this interval are queried
     * @return All the vehicles associated with the given portfolioId through their investments; the ids of those
     * investments are mapped to their vehicle; only includes the past prices that are included in the given interval
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create vehicles
     */
    private fun queryPortfolioInvestmentsVehicles(portfolioId: Int, interval: Interval): Map<Int, Vehicle> {
        val sql = """
            SELECT
                v.id AS id,
                v.symbol AS symbol,
                v.name AS name,
                i.id AS investment_id
            FROM $VEHICLE_TABLE AS v
            INNER JOIN $INVESTMENT_TABLE as i
                ON v.id = i.vehicle_id
            INNER JOIN $PORTFOLIO_TABLE as p
                ON i.portfolio_id = p.id
            WHERE p.id = ?
            """.trimIndent()

        val vehiclesPastPrices = queryPortfolioInvestmentsVehiclesPastPrices(portfolioId, interval)

        return query(sql, mapOf(), setPortfolioIdClosure(portfolioId)) { rs ->
            val vehicles = mutableMapOf<Int, Vehicle>()

            while (rs.next()) {
                val vehicleId = rs.getInt("vehicle_id")
                val pastPrices = vehiclesPastPrices[vehicleId] ?: listOf()
                val vehicle = buildVehicle(rs, pastPrices)

                val investmentId = rs.getInt("investment_id")
                vehicles[investmentId] = vehicle
            }

            vehicles
        }
    }

    /**
     * @param portfolioId A unique identifier of a portfolio; only past prices associated with this portfolio, through
     * its usd to base currency rate vehicle are queried
     * @param interval Only past prices that are included in this interval are queried
     * @return All the past prices associated with the given portfolioId through its usd to base currency rate vehicle;
     * only includes the past prices that are included in the given interval
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create past prices
     */
    private fun queryPortfolioUsdToBaseCurrencyRateVehiclePastPrices(portfolioId: Int, interval: Interval): List<PastPrice> {
        val sql = """
            SELECT
                pp.id AS id,
                pp.date_time AS date_time,
                pp.price AS price,
                pp.is_closing AS is_closing,
                pp.vehicle_id AS vehicle_id
            FROM $PAST_PRICE_TABLE AS pp
            INNER JOIN $VEHICLE_TABLE AS v
                ON pp.vehicle_id = v.id
            INNER JOIN $PORTFOLIO_TABLE as p
                ON v.id = p.usd_to_base_currency_rate_vehicle_id
            WHERE
                p.id = ?
                AND DATETIME(pp.date_time) BETWEEN DATETIME(?) AND DATETIME(?)
                AND pp.date_time LIKE %?
            """.trimIndent()

        // FIXME: verify the pp.date_time LIKE %? is correct

        return query(sql, listOf(), setPastPricesClosure(portfolioId, interval)) { rs ->
            val pastPrices = mutableListOf<PastPrice>()

            while (rs.next()) {
                val pastPrice = buildPastPrice(rs)
                pastPrices.add(pastPrice)
            }

            pastPrices
        }
    }

    /**
     * @param portfolioId A unique identifier of a portfolio; only past prices associated with this portfolio, through
     * their vehicles and through the investments of those vehicles are queried
     * @param interval Only past prices that are included in this interval are queried
     * @return All the past prices associated with the given portfolioId through their vehicles and through the
     * investments of those vehicles; only includes the past prices that are included in the given interval; these are
     * organized into sets which are mapped to the vehicle id that all past prices in the set correspond to
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create past prices
 */
    private fun queryPortfolioInvestmentsVehiclesPastPrices(
        portfolioId: Int,
        interval: Interval
    ): Map<Int, List<PastPrice>> {
        val sql = """
            SELECT
                pp.id AS id,
                pp.date_time AS date_time,
                pp.price AS price,
                pp.is_closing AS is_closing,
                pp.vehicle_id AS vehicle_id
            FROM $PAST_PRICE_TABLE AS pp
            INNER JOIN $VEHICLE_TABLE AS v
                ON pp.vehicle_id = v.id
            INNER JOIN $INVESTMENT_TABLE as i
                ON v.id = i.vehicle_id
            INNER JOIN $PORTFOLIO_TABLE as p
                ON i.portfolio_id = p.id
            WHERE
                p.id = ?
                AND DATETIME(pp.date_time) BETWEEN DATETIME(?) AND DATETIME(?)
                AND pp.date_time LIKE %?
        """.trimIndent()

        // FIXME: verify the pp.date_time LIKE %? is correct

        return query(sql, mapOf(), setPastPricesClosure(portfolioId, interval)) { rs ->
            val vehiclesPastPrices = mutableMapOf<Int, MutableList<PastPrice>>()

            while (rs.next()) {
                val pastPrice = buildPastPrice(rs)
                val vehicleId = pastPrice.vehicleId

                if (!vehiclesPastPrices.containsKey(vehicleId)) vehiclesPastPrices[vehicleId] = mutableListOf()
                vehiclesPastPrices[vehicleId]!!.add(pastPrice)
            }

            vehiclesPastPrices
        }
    }

    /**
     * TODO: write documentation
     */
    fun queryVehicles(query: String): List<Vehicle> = listOf()    // TODO: implement stub

    /**
     * TODO: write documentation
     */
    fun queryPastPrices(vehicleId: Int, interval: Interval): List<PastPrice> = listOf() // TODO: implement stub

    /**
     * Perform the given sql query
     *
     * @param sql The query to perform
     * @param default The default value produced if the query produces no results
     * @param setValuesClosure The function that sets the values in the given sql
     * @param resultSetExtractor Builds the value produced by this query
     * @return Produces the results of this query; if there are no results, produces the given default
     * @throws DataAccessException If the database was unable to perform the query
     */
    private fun <T> query(
        sql: String,
        default: T,
        setValuesClosure: (PreparedStatement) -> Unit,
        resultSetExtractor: (rs: ResultSet) -> Vehicle
    ): T = jdbcTemplate.query(prepareStatementClosure(sql), setValuesClosure, resultSetExtractor) ?: default
}