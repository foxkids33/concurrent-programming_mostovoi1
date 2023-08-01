@file:Suppress("unused")

package day4

import TestBase
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.paramgen.*

@Param(name = "index", gen = IntGen::class, conf = "0:3")
class DynamicArrayTest : TestBase(IntDynamicArraySequential::class) {
    private val array = DynamicArray<Int>()

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun addLast(element: Int) = array.addLast(element)

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun set(@Param(name = "index") index: Int, element: Int) = array.set(index, element)

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun get(@Param(name = "index") index: Int) = array.get(index)
}

class IntDynamicArraySequential {
    private val array = ArrayList<Int>()

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun addLast(element: Int) {
        array += element
    }

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun set(index: Int, element: Int) {
        require(index < array.size)
        array[index] = element
    }

    @Operation(handleExceptionsAsResult = [IllegalArgumentException::class])
    fun get(index: Int): Int {
        require(index < array.size)
        return array[index]
    }
}