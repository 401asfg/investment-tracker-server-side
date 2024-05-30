package com.michael.investmenttrackerserverside.controllers

import Portfolio
import Vehicle
import com.michael.investmenttrackerserverside.PORTFOLIO_RESOURCE
import com.michael.investmenttrackerserverside.services.DataManager
import com.michael.investmenttrackerserverside.services.Database
import com.michael.investmenttrackerserverside.services.QueryEngine
import org.springframework.web.bind.annotation.*

/**
 * The controller for portfolio
 */
@RestController
@RequestMapping(PORTFOLIO_RESOURCE)
class PortfolioController(private val dataManager: DataManager) {
    /**
     * TODO: write documentation
     */
    @GetMapping("/{id}", "application/json")
    fun show(@PathVariable("id") id: Int): Portfolio {
        return Portfolio(listOf(), Vehicle("", "", listOf()))   // TODO: implement stub
    }

    /**
     * TODO: write documentation
     */
    @PostMapping("/", "application/json")
    fun store(@RequestBody portfolio: Portfolio): Int {
        // FIXME: validate portfolio
        // FIXME: handle exceptions
        return 0    // TODO: implement stub
    }
}