package day3

import AbstractQueueTest
import org.jetbrains.kotlinx.lincheck.annotations.*

class MSQueueWithLinearTimeRemoveTest : AbstractQueueWithRemoveTest(MSQueueWithLinearTimeRemove())
class MSQueueWithConstantTimeRemoveTest : AbstractQueueWithRemoveTest(MSQueueWithConstantTimeRemove())


abstract class AbstractQueueWithRemoveTest(
    private val queue: QueueWithRemove<Int>,
    checkObstructionFreedom: Boolean = true
) : AbstractQueueTest(queue, checkObstructionFreedom) {
    @Operation
    fun remove(@Param(name = "element") element: Int) = queue.remove(element)

    @Validate
    fun checkNoRemovedElements() = queue.checkNoRemovedElements()

    @StateRepresentation
    fun state() = queue.toString()
}

