@file:Suppress("DuplicatedCode")

package day4

import day4.AtomicCounterArray.Status.*
import kotlinx.atomicfu.*
import javax.management.Descriptor

// This implementation never stores `null` values.
class AtomicCounterArray(size: Int) {
    private val array = atomicArrayOfNulls<Any>(size)

    init {
        // Fill array with zeros.
        for (i in 0 until size) {
            array[i].value = 0
        }
    }

    fun get(index: Int): Int {
        // TODO: the cell can store a descriptor.
        val currentValue = array[index].value

        return if (currentValue is IncrementDescriptor) {
            val statusValue = currentValue.status.value
            val incrementValue = if (index == currentValue.index1) currentValue.valueBeforeIncrement1
            else currentValue.valueBeforeIncrement2

            if (statusValue == SUCCESS) incrementValue + 1
            else incrementValue
        } else {
            currentValue as Int
        }
    }

    fun inc2(index1: Int, index2: Int) {
        require(index1 != index2) { "Indices must be distinct" }

        while (true) {
            val value1 = array[index1].value
            val value2 = array[index2].value

            // Check if there are pending operations, and apply them if found
            if (value1 is IncrementDescriptor) {
                value1.applyOperation()
            } else if (value2 is IncrementDescriptor) {
                value2.applyOperation()
            } else {
                // Prepare new operation descriptor, making sure the smaller index comes first
                val descriptor = if (index1 < index2) {
                    IncrementDescriptor(index1, value1 as Int, index2, value2 as Int)
                } else {
                    IncrementDescriptor(index2, value2 as Int, index1, value1 as Int)
                }

                val smallerIndex = minOf(index1, index2)
                val smallerValue = if (index1 < index2) value1 else value2

                // Attempt to place the descriptor into the smaller index cell
                if (array[smallerIndex].compareAndSet(smallerValue, descriptor)) {
                    // Execute operation and terminate if successful
                    descriptor.applyOperation()
                    if (descriptor.status.value == SUCCESS)
                        return
                }
            }
        }
    }


    // TODO: Implement the `inc2` operation using this descriptor.
    // TODO: 1) Read the current cell states
    // TODO: 2) Create a new descriptor
    // TODO: 3) Call `applyOperation()` -- it should try to increment the counters atomically.
    // TODO: 4) Check whether the `status` is `SUCCESS` or `FAILED`, restarting in the latter case.
    private inner class IncrementDescriptor(
        val index1: Int, val valueBeforeIncrement1: Int,
        val index2: Int, val valueBeforeIncrement2: Int
    ) {
        val status = atomic(UNDECIDED)

        // TODO: Other threads can call this function
        // TODO: to help completing the operation.
        // Function to apply the CAS2 algorithm to `array[index1]` and `array[index2]` cells.
        fun applyOperation() {
            while (true) {
                array[index2].compareAndSet(valueBeforeIncrement2, this)
                val resultValue = array[index2].value
                when (resultValue) {
                    // If the CAS operation succeeded for this descriptor, mark as SUCCESS.
                    this -> status.compareAndSet(UNDECIDED, SUCCESS)
                    // If the result is another descriptor, recursively apply the operation on that descriptor.
                    is IncrementDescriptor -> resultValue.applyOperation()
                    // If the value is valueBeforeIncrement2 + 1, check and update the status.
                    valueBeforeIncrement2 + 1 -> {
                        if (status.value != SUCCESS) {
                            status.value = FAILED
                        }
                    }
                    // If the value is different from the expected ones, mark as FAILED.
                    else -> status.compareAndSet(UNDECIDED, FAILED)
                }
                when (status.value) {
                    UNDECIDED -> {
                        status.compareAndSet(UNDECIDED, FAILED)
                        continue
                    }
                    FAILED -> break
                    SUCCESS -> { /* Do nothing for SUCCESS case */ }
                }

                if (array[index1].compareAndSet(this, valueBeforeIncrement1 + 1)) {
                    continue
                }
                
                if (array[index2].compareAndSet(this, valueBeforeIncrement2 + 1)) {
                    continue
                }

                // If both updates failed, exit the loop.
                break
            }

            // If the status is FAILED, revert the CAS operations on `array[index1]` and `array[index2]`.
            if (status.value == FAILED) {
                array[index2].compareAndSet(this, valueBeforeIncrement2)
                array[index1].compareAndSet(this, valueBeforeIncrement1)
            }
        }

    }

    private enum class Status {
        UNDECIDED, FAILED, SUCCESS
    }
}