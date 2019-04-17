package kotlinx.coroutines.sync

import kotlinx.atomicfu.*
import kotlinx.coroutines.CancelHandler
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.asHandler
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.suspendAtomicCancellableCoroutine
import kotlin.coroutines.resume
import kotlin.jvm.JvmField
import kotlin.math.max

public interface Semaphore {
    public val availablePermits: Int
    public fun tryAcquire(): Boolean
    public suspend fun acquire()
    public fun release()
}

public fun Semaphore(maxPermits: Int, acquiredPermits: Int = 0): Semaphore = SemaphoreImpl(maxPermits, acquiredPermits)

public suspend inline fun <T> Semaphore.withSemaphore(action: () -> T): T {
    acquire()
    try {
        return action()
    } finally {
        release()
    }
}

private class SemaphoreImpl(@JvmField val maxPermits: Int, acquiredPermits: Int)
    : Semaphore, SegmentQueue<SemaphoreSegment>(createFirstSegment = false)
{
    init {
        require(maxPermits > 0) { "Semaphore should have at least 1 permit"}
        require(acquiredPermits in 0..maxPermits) { "TODO" }
    }

    override fun newSegment(id: Long, prev: SemaphoreSegment?)= SemaphoreSegment(id, prev)

    private val _availablePermits = atomic(maxPermits)
    override val availablePermits: Int get() = max(_availablePermits.value.toInt(), 0)

    // Queue of waiting `acquire` requests. The queue is represented via enqueue and dequeue
    // indices (`enqIdx` and `deqIdx` respectively) and infinite array. Each enqueue and dequeue
    // operations increment the corresponding counter and goes to the corresponding unique cell.
    // Due to the fact that we do not need to really send anything through this queue, we can make
    // this queue wait-free. In order to do this,

    private val enqIdx = atomic(0L)
    private val deqIdx = atomic(0L)

    override fun tryAcquire(): Boolean {
        _availablePermits.loop { p ->
            if (p <= 0) return false
            if (_availablePermits.compareAndSet(p, p - 1)) return true
        }
    }

    override suspend fun acquire() {
        val p = _availablePermits.getAndDecrement()
        if (p > 0) return // permit acquired
        addToQueueAndSuspend()
    }

    override fun release() {
        val p = _availablePermits.getAndUpdate { cur ->
            check(cur < maxPermits) { "Cannot TODO" }
            cur + 1
        }
        if (p >= 0) return // no waiters
        resumeNextFromQueue()
    }

    private suspend fun addToQueueAndSuspend() = suspendAtomicCancellableCoroutine<Unit> sc@ { cont ->
        val tail = this.tail
        val enqIdx = enqIdx.getAndIncrement()
        val segment = getSegment(tail, enqIdx / SEGMENT_SIZE)
        val i = (enqIdx % SEGMENT_SIZE).toInt()
        if (segment === null || segment[i].value === RESUMED || !segment[i].compareAndSet(null, cont)) {
            cont.resume(Unit)
            return@sc
        }
        cont.invokeOnCancellation(handler = object : CancelHandler() {
            override fun invoke(cause: Throwable?) {
                segment.clean(i)
                release()
            }
        }.asHandler)
    }

    private fun resumeNextFromQueue() {
        val head = this.head
        val deqIdx = deqIdx.getAndIncrement()
        val segment = getSegmentAndMoveHeadForward(head, deqIdx / SEGMENT_SIZE) ?: return
        val i = (deqIdx % SEGMENT_SIZE).toInt()
        val cont = segment[i].getAndUpdate {
            if (it === CLEANED) it else RESUMED
        }
        if (cont === CLEANED) return
        cont as CancellableContinuation<Unit>
        cont.resume(Unit)
    }
}

private class SemaphoreSegment(id: Long, prev: SemaphoreSegment?): Segment<SemaphoreSegment>(id, prev) {
    // == Waiters Array ==
    private val waiters = atomicArrayOfNulls<Any?>(SEGMENT_SIZE)

    operator fun get(index: Int): AtomicRef<Any?> = waiters[index]

    private val cleaned = atomic(0)
    override val removed get() = cleaned.value == SEGMENT_SIZE

    /**
     * Cleans the waiter located by the specified index in this segment.
     */
    fun clean(index: Int) {
        // Clean the specified waiter and
        // check if all node items are cleaned.
        waiters[index].value = CLEANED
        // Remove this segment if needed
        if (cleaned.incrementAndGet() == SEGMENT_SIZE)
            remove()
    }
}

@SharedImmutable
private val RESUMED = Symbol("RESUMED")
@SharedImmutable
private val CLEANED = Symbol("CLEANED")
@SharedImmutable
private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.semaphore.segmentSize", 32)