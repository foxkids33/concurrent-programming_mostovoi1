@file:Suppress("unused")

package day4

import TestBase
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.paramgen.*

@Param(name = "index", gen = IntGen::class, conf = "0:3")
class DynamicArraySimplifiedTest : TestBase(IntDynamicArraySimplifiedSequential::class) {
    private val array = DynamicArraySimplified<Int>(CAPACITY)

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun addLast(element: Int): Boolean = array.addLast(element)

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun set(@Param(name = "index") index: Int, element: Int) = array.set(index, element)

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun get(@Param(name = "index") index: Int) = array.get(index)
}

class IntDynamicArraySimplifiedSequential {
    private val array = ArrayList<Int>()

    fun addLast(element: Int): Boolean =
        if (array.size == CAPACITY) {
            false
        } else {
            array += element
            true
        }

    fun set(index: Int, element: Int) {
        require(index < array.size)
        array[index] = element
    }

    fun get(index: Int): Int {
        require(index < array.size)
        return array[index]
    }
}

private const val CAPACITY = 3