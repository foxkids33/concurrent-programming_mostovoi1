package day4

import kotlinx.atomicfu.*

class DynamicArray<E: Any> {
    private val core = atomic(Core(capacity = 1))

    @JvmInline
    private value class Frozen(val element: Any)

    private class Core(val capacity: Int) {
        val array = atomicArrayOfNulls<Any?>(capacity)
        val size = atomic(0)
        val next = atomic<Core?>(null)
    }

    private fun createNewCore(curCore: Core): Core {
        val newCore = Core(curCore.capacity * 2)
        newCore.size.value = curCore.size.value
        curCore.next.compareAndSet(null, newCore)
        return newCore
    }

    private fun copyToNextCore(curCore: Core, nextCore: Core) {
        var index = 0
        while (index < curCore.size.value) {
            val curElement = curCore.array[index].value
            when (curElement) {
                is Frozen -> {
                    nextCore.array[index].compareAndSet(null, curElement.element)
                    index++
                }
                else -> {
                    curCore.array[index].compareAndSet(curElement, Frozen(curElement!!))
                }
            }
        }
        core.compareAndSet(curCore, nextCore)
    }

    fun addLast(element: E) {
        while (true) {
            val curCore = core.value
            val nextCore = curCore.next.value

            if (nextCore == null) {
                val curSize = curCore.size.value
                if (curSize == curCore.capacity) {
                    createNewCore(curCore)
                    continue
                }
                if (!curCore.array[curSize].compareAndSet(null, element)) {
                    curCore.size.compareAndSet(curSize, curSize+1)
                    continue
                }
                while (true) {
                    curCore.size.compareAndSet(curSize, curSize + 1)
                    if (curCore.size.value > curSize) return
                }
            } else {
                copyToNextCore(curCore, nextCore)
            }
        }
    }

    fun set(index: Int, element: E) {
        while (true) {
            val curCore = core.value
            val curSize = curCore.size.value

            require(index < curSize) { "index must be lower than the array size" }

            val curCellValue = curCore.array[index].value
            when (curCellValue) {
                is Frozen -> {
                    val nextCore = curCore.next.value!!
                    copyToNextCore(curCore, nextCore)
                    continue
                }
                else -> {
                    if (!curCore.array[index].compareAndSet(curCellValue, element)) continue
                    return
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun get(index: Int): E {
        val curCore = core.value
        val curSize = curCore.size.value

        require(index < curSize) { "index must be lower than the array size" }

        return when(val curCellValue = curCore.array[index].value) {
            is Frozen -> curCellValue.element as E
            else -> curCellValue as E
        }
    }
}
