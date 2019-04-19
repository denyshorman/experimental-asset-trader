package com.gitlab.dhorman.cryptotrader.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob

val FlowScope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
