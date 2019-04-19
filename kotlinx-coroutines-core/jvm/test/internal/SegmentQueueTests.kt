package kotlinx.coroutines.internal

import com.devexperts.dxlab.lincheck.LinChecker
import com.devexperts.dxlab.lincheck.annotations.Operation
import com.devexperts.dxlab.lincheck.annotations.Param
import com.devexperts.dxlab.lincheck.paramgen.IntGen
import com.devexperts.dxlab.lincheck.strategy.stress.StressCTest
import kotlinx.atomicfu.atomic
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.*
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private class OneElementSegment<T>(id: Long, prev: OneElementSegment<T>?) : Segment<OneElementSegment<T>>(id, prev) {
    val element = atomic<Any?>(null)

    override val removed get() = element.value === REMOVED

    fun removeSegmentLogically() {
        element.value = REMOVED
    }

    fun removeSegmentPhysically() {
        remove()
    }
}

private class SegmentBasedQueue<T>(createFirstSegment: Boolean) : SegmentQueue<OneElementSegment<T>>(createFirstSegment) {
    override fun newSegment(id: Long, prev: OneElementSegment<T>?): OneElementSegment<T> = OneElementSegment(id, prev)

    private val enqIdx = atomic(0L)
    private val deqIdx = atomic(0L)

    fun add(element: T): OneElementSegment<T> {
        while (true) {
            var tail = this.tail
            val enqIdx = this.enqIdx.getAndIncrement()
            tail = getSegment(tail, enqIdx) ?: continue
            if (tail.element.value === BROKEN) continue
            if (tail.element.compareAndSet(null, element)) return tail
        }
    }

    fun poll(): T? {
        while (true) {
            if (this.deqIdx.value >= this.enqIdx.value) return null
            var head = this.head
            val deqIdx = this.deqIdx.getAndIncrement()
            head = getSegmentAndMoveHeadForward(head, deqIdx) ?: continue
            var el = head.element.value
            if (el === null) {
                if (head.element.compareAndSet(null, BROKEN)) continue
                else el = head.element.value
            }
            if (el === REMOVED) continue
            return el as T
        }
    }

    val numberOfSegments: Int get() {
        var s: OneElementSegment<T>? = head
        var i = 0
        while (s != null) {
            s = s.next.value
            i++
        }
        return i
    }
}

private val BROKEN = Symbol("BROKEN")
private val REMOVED = Symbol("REMOVED")

@RunWith(Parameterized::class)
class SegmentQueueTest(private val createFirstSegment: Boolean) {
    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "createFirstSegment={0}")
        fun testArguments() = listOf(true, false)
    }

    @Test
    fun simpleTest() {
        val q = SegmentBasedQueue<Int>(createFirstSegment)
        assertEquals(if (createFirstSegment) 1 else 0, q.numberOfSegments)
        assertEquals(null, q.poll())
        q.add(1)
        assertEquals(1, q.numberOfSegments)
        q.add(2)
        assertEquals(2, q.numberOfSegments)
        assertEquals(1, q.poll())
        assertEquals(2, q.numberOfSegments)
        assertEquals(2, q.poll())
        assertEquals(1, q.numberOfSegments)
        assertEquals(null, q.poll())

    }

    @Test
    fun testSegmentRemoving() {
        val q = SegmentBasedQueue<Int>(createFirstSegment)
        q.add(1)
        val s = q.add(2)
        q.add(3)
        assertEquals(3, q.numberOfSegments)
        s.removeSegmentLogically()
        s.removeSegmentPhysically()
        assertEquals(2, q.numberOfSegments)
        assertEquals(1, q.poll())
        assertEquals(3, q.poll())
        assertEquals(null, q.poll())
    }

    @Test
    fun testRemoveHeadSegment() {
        val q = SegmentBasedQueue<Int>(createFirstSegment)
        q.add(1)
        val s = q.add(2)
        assertEquals(1, q.poll())
        q.add(3)
        s.removeSegmentLogically()
        s.removeSegmentPhysically()
        assertEquals(3, q.poll())
        assertEquals(null, q.poll())
    }

    @Test
    fun testRemoveHeadLogically() {
        val q = SegmentBasedQueue<Int>(createFirstSegment)
        val s = q.add(1)
        s.removeSegmentLogically()
        assertEquals(null, q.poll())
    }

    @Test
    fun stressTest() {
        val q = SegmentBasedQueue<Int>(createFirstSegment)
        val expectedQueue = ArrayDeque<Int>()
        val r = Random(0)
        repeat(1_000_000) {
            if (r.nextBoolean()) { // add
                val el = r.nextInt()
                q.add(el)
                expectedQueue.add(el)
            } else { // remove
                assertEquals(expectedQueue.poll(), q.poll())
            }
        }
    }

    @Test
    fun stressTestRemoveSegmentsSerial() = stressTestRemoveSegments(false)

    @Test
    fun stressTestRemoveSegmentsRandom() = stressTestRemoveSegments(true)

    private fun stressTestRemoveSegments(random: Boolean) {
        val N = 100_000
        val T = 1
        val q = SegmentBasedQueue<Int>(createFirstSegment)
        val segments = (1..N).map { q.add(it) }.toMutableList()
        if (random) segments.shuffle()
        assertEquals(N, q.numberOfSegments)
        val nextSegmentIndex = AtomicInteger()
        val barrier = CyclicBarrier(T)
        (1..T).map {
            thread {
                while (true) {
                    barrier.await()
                    val i = nextSegmentIndex.getAndIncrement()
                    if (i >= N) break
                    segments[i].removeSegmentLogically()
                     assertTrue(segments[i].removed)
                    segments[i].removeSegmentPhysically()
                }
            }
        }.forEach { it.join() }
        assertEquals(2, q.numberOfSegments)
    }
}

@StressCTest
class SegmentQueueLFTest {
    private companion object {
        var createFirstSegment: Boolean = false
    }

    private val q = SegmentBasedQueue<Int>(createFirstSegment)

    @Volatile
    private var removedSegment1: OneElementSegment<Int>? = null
    @Volatile
    private var removedSegment2: OneElementSegment<Int>? = null
    @Volatile
    private var lastAddedSegment: OneElementSegment<Int>? = null

    @Operation
    fun addAndSaveSegment(@Param(gen = IntGen::class) x: Int) {
        lastAddedSegment = q.add(x)
    }

    @Operation
    fun add(@Param(gen = IntGen::class) x: Int) {
        q.add(x)
    }

    @Operation
    fun removeSegmentLogically1() {
        val s = lastAddedSegment ?: return
        s.removeSegmentLogically()
        removedSegment1 = s
    }

    @Operation
    fun removeSegmentPhysically1() {
        val s = removedSegment1 ?: return
        s.remove()
    }

    @Operation
    fun removeSegmentLogically2() {
        val s = lastAddedSegment ?: return
        s.removeSegmentLogically()
        removedSegment2 = s
    }

    @Operation
    fun removeSegmentPhysically2() {
        val s = removedSegment2 ?: return
        s.remove()
    }

    @Operation
    fun remove(): Int? = q.poll()

    @Test
    fun test() {
        createFirstSegment = true
        LinChecker.check(SegmentQueueLFTest::class.java)
    }

    @Test
    fun testWithLazyFirstSegment() {
        createFirstSegment = false
        LinChecker.check(SegmentQueueLFTest::class.java)
    }
}