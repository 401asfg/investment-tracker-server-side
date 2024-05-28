package com.michael.investmenttrackerserverside.controllers

import PastPrice
import com.michael.investmenttrackerserverside.PAST_PRICE_RESOURCE
import com.michael.investmenttrackerserverside.services.QueryEngine
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

/**
 * The controller for past prices
 */
@RestController
@RequestMapping(PAST_PRICE_RESOURCE)
class PastPriceController(private val queryEngine: QueryEngine) {
    /**
     * TODO: write documentation
     */
    @GetMapping("/", "application/json")
    fun index(
        @RequestParam("portfolio_id") portfolioId: Int,
        @RequestParam("from") from: String,
        @RequestParam("to") to: String,
        @RequestParam("time_granularity") timeGranularity: String
    ): List<PastPrice> {
        return listOf() // TODO: implement stub
    }
}