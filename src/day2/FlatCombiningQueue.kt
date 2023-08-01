package day2

import day1.*
import kotlinx.atomicfu.*
import java.util.concurrent.*

class FlatCombiningQueue<E> : Queue<E> {
    private val queue = ArrayDeque<E>() // sequential queue
    private val combinerLock = atomic(false) // unlocked initially
    private val tasksForCombiner = atomicArrayOfNulls<Any?>(TASKS_FOR_COMBINER_SIZE)

    data class Element<E>(val value: E?, val push: Boolean)

    override fun enqueue(element: E) {
        var index = randomCellIndex()
        var pushed = false
        while (!combinerLock.compareAndSet(false, true)) {
            if (pushed) {
                if (tasksForCombiner[index].compareAndSet(PROCESSED, null)) {
                    return
                }
            } else {
                if (tasksForCombiner[index].compareAndSet(null, Element(element, true))) {
                    pushed = true
                }
                else index = randomCellIndex()
            }
        }
        processTasks()
        if (!pushed) queue.addLast(element)
        combinerLock.compareAndSet(true, false)
    }

    override fun dequeue(): E? {
        var index = randomCellIndex()
        var pushed = false
        while (!combinerLock.compareAndSet(false, true)) {
            if (pushed) {
                val element = tasksForCombiner[index].value
                if (element is Element<*>) {
                    tasksForCombiner[index].compareAndSet(element, null)
                    return element.value as E?
                }
            } else {
                if (tasksForCombiner[index].compareAndSet(null, DEQUE_TASK)) pushed = true
                else index = randomCellIndex()
            }
        }
        processTasks()
        val element: E?
        if (!pushed) {
            element = queue.removeFirstOrNull()
        } else {
            val node = tasksForCombiner[index].value as Element<E>
            element = node.value
            tasksForCombiner[index].compareAndSet(node, null)
        }
        combinerLock.compareAndSet(true, false)
        return element
    }

    private fun processTasks() {
        for (i in 0..(tasksForCombiner.size - 1)) {
            val curel = tasksForCombiner[i].value ?: continue
            if (curel is Element<*> && curel.push) {
                queue.addLast(curel.value as E)
                tasksForCombiner[i].compareAndSet(curel, PROCESSED)
            }
            if (curel == DEQUE_TASK) {
                tasksForCombiner[i].compareAndSet(DEQUE_TASK, Element(queue.removeFirstOrNull(), false))
            }
        }
    }

    private fun randomCellIndex(): Int =
            ThreadLocalRandom.current().nextInt(tasksForCombiner.size)
}

private const val TASKS_FOR_COMBINER_SIZE = 3 // Do not change this constant!

// TODO: Put this token in `tasksForCombiner` for dequeue().
// TODO: enqueue()-s should put the inserting element.
private val DEQUE_TASK = Any()

// TODO: Put this token in `tasksForCombiner` when the task is processed.
private val PROCESSED = Any()
