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
// FIXME: verify queries using "LIKE" are correct

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
        /* TABLE NAMES */
        const val PAST_PRICE_TABLE = "past_prices"
        const val VEHICLE_TABLE = "vehicles"
        const val INVESTMENT_TABLE = "investments"
        const val PORTFOLIO_TABLE = "portfolios"

        /* PORTFOLIO QUERY CLAUSES */
        private const val PORTFOLIO_ID_PARAM_WHERE_CLAUSE = "WHERE p.id = ?"

        private const val PORTFOLIO_SELECT_STATEMENT =
            "SELECT p.id FROM $PORTFOLIO_TABLE AS p $PORTFOLIO_ID_PARAM_WHERE_CLAUSE"

        /* INVESTMENT QUERY CLAUSES */
        private const val INVESTMENT_TO_PORTFOLIO_JOIN_CLAUSE =
            "INNER JOIN $PORTFOLIO_TABLE AS p ON i.portfolio_id = p.id"

        private const val INVESTMENT_SELECT_STATEMENT =
            "SELECT i.id, i.date_time, i.principal, i.vehicle_id, i.portfolio_id FROM $INVESTMENT_TABLE AS i " +
            "$INVESTMENT_TO_PORTFOLIO_JOIN_CLAUSE $PORTFOLIO_ID_PARAM_WHERE_CLAUSE"

        /* VEHICLE QUERY CLAUSES */
        private const val VEHICLE_SELECT_CLAUSE = "SELECT v.id, v.symbol, v.name"
        private const val VEHICLE_FROM_CLAUSE = "FROM $VEHICLE_TABLE AS v"
        private const val VEHICLE_SELECT_FROM_CLAUSE = "$VEHICLE_SELECT_CLAUSE $VEHICLE_FROM_CLAUSE"

        private const val VEHICLE_TO_INVESTMENT_TO_PORTFOLIO_JOIN_CLAUSE =
            "INNER JOIN $INVESTMENT_TABLE AS i ON v.id = i.vehicle_id $INVESTMENT_TO_PORTFOLIO_JOIN_CLAUSE"

        private const val USD_TO_BASE_CURRENCY_RATE_VEHICLE_TO_PORTFOLIO_JOIN_CLAUSE =
            "INNER JOIN $PORTFOLIO_TABLE AS p ON v.id = p.usd_to_base_currency_rate_vehicle_id"

        /* PAST PRICE QUERY CLAUSES */
        private const val PAST_PRICE_SELECT_FROM_CLAUSE =
            "SELECT pp.id, pp.date_time, pp.price, pp.is_closing, pp.vehicle_id FROM $PAST_PRICE_TABLE AS pp"

        private const val PAST_PRICE_TO_VEHICLE_JOIN_CLAUSE = "INNER JOIN $VEHICLE_TABLE AS v ON pp.vehicle_id = v.id"

        // FIXME: use is_closing

        private const val PAST_PRICE_WHERE_CLAUSE =
            "$PORTFOLIO_ID_PARAM_WHERE_CLAUSE " +
            "AND DATETIME(pp.date_time) BETWEEN DATETIME(?) AND DATETIME(?) AND pp.date_time LIKE %?"

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
         * Sets an id in the given preparedStatement with the value of the given id
         *
         * @param preparedStatement The statement to set the id of
         * @param id The value to set the id in the statement to
         * @throws SQLException If the statement contains no id
         */
        private fun setId(preparedStatement: PreparedStatement, id: Int) {
            preparedStatement.setInt(1, id)
        }

        /**
         * @param id The id to set in a prepared statement
         * @return A function that sets the id in a prepared statement
         */
        private fun setIdInPreparedStatement(id: Int): (PreparedStatement) -> Unit = { ps -> setId(ps, id) }

        /**
         * @param id The id to set in a prepared statement
         * @param interval The interval to set in a prepared statement
         * @return A function that sets the id and interval fields in a prepared statement
         */
        private fun setPastPriceParametersInPreparedStatement(
            id: Int,
            interval: Interval
        ): (PreparedStatement) -> Unit
            = { ps ->
                val fromDate = interval.from.toTimestamp()
                val toDate = interval.to.toTimestamp()
                val requiredTimestampEnd = interval.requiredTimestampEnd

                setId(ps, id)
                ps.setString(2, fromDate)
                ps.setString(3, toDate)
                ps.setString(4, requiredTimestampEnd)
            }

        // FIXME: make sure build fns should take in maps and not just what they need

        /**
         * @param resultSet The data to build a portfolio from
         * @param investments The portfolio's investments
         * @param usdToBaseCurrencyRateVehicle The portfolio's usd to base currency rate vehicle
         * @return A portfolio from the given resultSet data
         * @throws SQLException If the given resultSet didn't contain some of the data needed to build a portfolio
         */
        private fun buildPortfolio(
            resultSet: ResultSet,
            investments: List<Investment>,
            usdToBaseCurrencyRateVehicle: Vehicle
        ): Portfolio {
            val id = resultSet.getInt("id")
            return Portfolio(investments, usdToBaseCurrencyRateVehicle, id)
        }

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

        /**
         * @param resultSet The data to build a list item from
         * @param build A function that builds each item in the produced list
         * @return A list of items, each built using the data from the given resultSet and built by the given build
         * function
         */
        private fun <T> buildList(resultSet: ResultSet, build: () -> T): List<T> {
            val list = mutableListOf<T>()

            while (resultSet.next()) {
                val item = build()
                list.add(item)
            }

            return list
        }

        /**
         * @param resultSet The data to build a map item from
         * @param updateMap Updates the map for each set of data in the given resultSet
         * @return A map with ids as keys corresponding to values; The map is updated for each set of data in the given
         * resultSet
         */
        private fun <T> buildIdMap(
            resultSet: ResultSet,
            updateMap: (MutableMap<Int, T>) -> Unit
        ): Map<Int, T> {
            val map = mutableMapOf<Int, T>()
            while (resultSet.next()) updateMap(map)
            return map
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

    // FIXME: refactor sql statements to use single point of control?

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
     * @throws MissingPortfolioException If all the queried portfolio couldn't be found in the database
     */
    fun queryPortfolio(id: Int, interval: Interval): Portfolio {
        val investments = queryPortfolioInvestments(id, interval)
        val usdToBaseCurrencyRateVehicle = queryPortfolioUsdToBaseCurrencyRateVehicle(id, interval)

        return query(
            PORTFOLIO_SELECT_STATEMENT,
            id,
            { rs -> buildPortfolio(rs, investments, usdToBaseCurrencyRateVehicle) },
            {
                throw MissingPortfolioException("Query for a portfolio with the id: $id didn't produce any results")
            },
            {
                throw MissingPortfolioException("Query for a portfolio with the id: $id produced more than one entry")
            }
        )
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
        val investmentsVehicles = queryPortfolioInvestmentsVehicles(portfolioId, interval)

        return query(INVESTMENT_SELECT_STATEMENT, listOf(), setIdInPreparedStatement(portfolioId)) { rs ->
            buildList(rs) {
                val vehicleId = rs.getInt("vehicle_id")

                val vehicle = investmentsVehicles[vehicleId]
                    ?: throw MissingVehicleException("Query for investment couldn't find a corresponding vehicle")

                buildInvestment(rs, vehicle)
            }
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
            $VEHICLE_SELECT_FROM_CLAUSE
            $USD_TO_BASE_CURRENCY_RATE_VEHICLE_TO_PORTFOLIO_JOIN_CLAUSE
            $PORTFOLIO_ID_PARAM_WHERE_CLAUSE
        """.trimIndent()

        val pastPrices = queryPortfolioUsdToBaseCurrencyRateVehiclePastPrices(portfolioId, interval)

        return query(
            sql,
            portfolioId,
            { rs -> buildVehicle(rs, pastPrices) },
            {
                throw MissingVehicleException(
                    "Query for a portfolio's usd to base currency rate vehicle didn't produce any results"
                )
            },
            {
                throw MissingVehicleException(
                    "Query for a portfolio's usd to base currency rate vehicle produced more than one result"
                )
            }
        )
    }

    /**
     * @param sql The sql query to execute
     * @param id The id to query for
     * @param build The function that builds the result from the query
     * @param throwNoResultsException Throws an exception if no results are found
     * @param throwMultipleResultsException Throws an exception if multiple results are found
     * @return The result of the given sql query
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create the result
     */
    private fun <T> query(
        sql: String,
        id: Int,
        build: (ResultSet) -> T,
        throwNoResultsException: () -> Unit,
        throwMultipleResultsException: () -> Unit
    ): T = query(sql, null, setIdInPreparedStatement(id)) { rs ->
        if (!rs.next()) throwNoResultsException()
        val result = build(rs)
        if (rs.next()) throwMultipleResultsException()
        result
    }!!

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
            $VEHICLE_SELECT_CLAUSE, i.id AS investment_id
            $VEHICLE_FROM_CLAUSE
            $VEHICLE_TO_INVESTMENT_TO_PORTFOLIO_JOIN_CLAUSE
            $PORTFOLIO_ID_PARAM_WHERE_CLAUSE
        """.trimIndent()

        val vehiclesPastPrices = queryPortfolioInvestmentsVehiclesPastPrices(portfolioId, interval)

        return query(sql, mapOf(), setIdInPreparedStatement(portfolioId)) { rs ->
            buildIdMap(rs) { map ->
                val vehicleId = rs.getInt("vehicle_id")
                val pastPrices = vehiclesPastPrices[vehicleId] ?: listOf()
                val vehicle = buildVehicle(rs, pastPrices)

                val investmentId = rs.getInt("investment_id")
                map[investmentId] = vehicle
            }
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
    private fun queryPortfolioUsdToBaseCurrencyRateVehiclePastPrices(
        portfolioId: Int,
        interval: Interval
    ): List<PastPrice> {
        val sql = """
            $PAST_PRICE_SELECT_FROM_CLAUSE
            $PAST_PRICE_TO_VEHICLE_JOIN_CLAUSE
            $USD_TO_BASE_CURRENCY_RATE_VEHICLE_TO_PORTFOLIO_JOIN_CLAUSE
            $PAST_PRICE_WHERE_CLAUSE
        """.trimIndent()

        return query(sql, listOf(), setPastPriceParametersInPreparedStatement(portfolioId, interval)) { rs ->
            buildList(rs) { buildPastPrice(rs) }
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
            $PAST_PRICE_SELECT_FROM_CLAUSE
            $PAST_PRICE_TO_VEHICLE_JOIN_CLAUSE
            $VEHICLE_TO_INVESTMENT_TO_PORTFOLIO_JOIN_CLAUSE
            $PAST_PRICE_WHERE_CLAUSE
        """.trimIndent()

        // FIXME: verify the pp.date_time LIKE %? is correct

        return query(sql, mapOf(), setPastPriceParametersInPreparedStatement(portfolioId, interval)) { rs ->
            buildIdMap<MutableList<PastPrice>>(rs) { map ->
                val pastPrice = buildPastPrice(rs)
                val vehicleId = pastPrice.vehicleId

                if (!map.containsKey(vehicleId)) map[vehicleId] = mutableListOf()
                map[vehicleId]!!.add(pastPrice)
            }
        }
    }

    /**
     * @param query The query by which to search for vehicles to produce
     * @return All the vehicles that contain the given query in their symbol or name
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create vehicles
     */
    fun queryVehicles(query: String): List<Vehicle> {
        val sql = """
            $VEHICLE_SELECT_FROM_CLAUSE
            WHERE
                v.symbol LIKE %?%
                OR v.name LIKE %?%
        """.trimIndent()

        return query(
            sql,
            listOf(),
            { ps ->
                ps.setString(1, query)
                ps.setString(2, query)
            },
            { rs -> buildList(rs) { buildVehicle(rs, listOf()) } }
        )
    }

    /**
     * @param vehicleId A unique identifier of a vehicle; only past prices associated with this vehicle are queried
     * @param interval Only past prices that are included in this interval are queried
     * @return All the past prices associated with the given vehicleId; only includes the past prices that are included
     * in the given interval
     * @throws DataAccessException If the database was unable to perform the query
     * @throws SQLException If the query didn't contain the correct data to create past prices
     */
    fun queryPastPrices(vehicleId: Int, interval: Interval): List<PastPrice> {
        val sql = """
            $PAST_PRICE_SELECT_FROM_CLAUSE
            $PAST_PRICE_WHERE_CLAUSE
        """.trimIndent()

        return query(sql, listOf(), setPastPriceParametersInPreparedStatement(vehicleId, interval)) { rs ->
            buildList(rs) { buildPastPrice(rs) }
        }
    }

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
        resultSetExtractor: (rs: ResultSet) -> T
    ): T = jdbcTemplate.query(prepareStatementClosure(sql), setValuesClosure, resultSetExtractor) ?: default
}