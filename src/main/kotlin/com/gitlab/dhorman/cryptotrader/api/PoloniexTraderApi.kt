package com.gitlab.dhorman.cryptotrader.api

import io.swagger.annotations.ApiOperation
import io.vavr.collection.List
import io.vavr.kotlin.list
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping(value = ["/api/traders/poloniex"], produces = [MediaType.APPLICATION_JSON_UTF8_VALUE])
class PoloniexTraderApi {
    @ApiOperation(
        value = "Retrieve hello list",
        notes = "Use this resource to retrieve hello list"
    )
    @RequestMapping(method = [RequestMethod.GET], value = ["/hello"])
    fun hello(): List<String> {
        return list("one", "two", "three")
    }
}