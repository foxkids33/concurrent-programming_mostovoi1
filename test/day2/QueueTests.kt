package day2

import AbstractQueueTest

class FAABasedQueueSimplifiedTest : AbstractQueueTest(FAABasedQueueSimplified())
class FAABasedQueueTest : AbstractQueueTest(FAABasedQueue())

class FlatCombiningQueueTest : AbstractQueueTest(FlatCombiningQueue(), checkObstructionFreedom = false)

