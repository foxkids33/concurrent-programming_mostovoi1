@file:Suppress("ReplaceGetOrSet", "unused")

package day4

import TestBase
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.paramgen.*
import org.jetbrains.kotlinx.lincheck.verifier.*

@Param(name = "key", gen = IntGen::class, conf = "1:8")
@Param(name = "value", gen = IntGen::class, conf = "1:10")
class IntIntHashMapTest : TestBase(IntIntHashMapSequential::class) {
    private val map = IntIntHashMap()
    
    @Operation
    fun put(@Param(name = "key") key: Int, @Param(name = "value") value: Int): Int = map.put(key, value)

    @Operation
    fun remove(@Param(name = "key") key: Int): Int = map.remove(key)

    @Operation
    fun get(@Param(name = "key") key: Int): Int = map.get(key)
}

class IntIntHashMapSequential : VerifierState() {
    private val map = HashMap<Int, Int>()

    fun put(key: Int, value: Int): Int = map.put(key, value) ?: 0
    fun remove(key: Int): Int = map.remove(key) ?: 0
    fun get(key: Int): Int = map.get(key) ?: 0

    override fun extractState() = map
}