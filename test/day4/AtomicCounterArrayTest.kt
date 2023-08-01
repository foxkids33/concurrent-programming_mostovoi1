package day4

import TestBase
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.paramgen.*

@Param(name = "index", gen = IntGen::class, conf = "0:${ARRAY_SIZE - 1}")
class AtomicCounterArrayTest : TestBase(AtomicCounterArraySequential::class) {
    private val counters = AtomicCounterArray(ARRAY_SIZE)

    @Operation
    fun get(@Param(name = "index") index: Int) = counters.get(index)

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun inc2(@Param(name = "index") index1: Int, @Param(name = "index") index2: Int) = counters.inc2(index1, index2)
}

class AtomicCounterArraySequential {
    private val array = IntArray(ARRAY_SIZE)

    fun get(index: Int) = array[index]

    fun inc2(index1: Int, index2: Int) {
        require(index1 != index2) { "The indices should be different" }
        array[index1]++
        array[index2]++
    }
}

private const val ARRAY_SIZE = 3