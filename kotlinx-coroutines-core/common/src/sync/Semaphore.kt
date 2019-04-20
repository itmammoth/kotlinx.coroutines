package kotlinx.coroutines.sync

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.selects.select
import kotlin.coroutines.resume
import kotlin.jvm.JvmField
import kotlin.math.max

/**
 * A counting semaphore for coroutines. It maintains a number of available permits.
 * Each [acquire] suspends if necessary until a permit is available, and then takes it.
 * Each [release] adds a permit, potentially releasing a suspended acquirer.
 *
 * Semaphore with `maxPermits = 1` is essentially a [Mutex].
 **/
public interface Semaphore {
    /**
     * Returns the current number of available permits available in this semaphore.
     */
    public val availablePermits: Int

    /**
     * Acquires a permit from this semaphore, suspending until one is available.
     * All suspending acquirers are processed in first-in-first-out (FIFO) order.
     *
     * This suspending function is cancellable. If the [Job] of the current coroutine is cancelled or completed while this
     * function is suspended, this function immediately resumes with [CancellationException].
     *
     * *Cancellation of suspended lock invocation is atomic* -- when this function
     * throws [CancellationException] it means that the mutex was not locked.
     *
     * Note, that this function does not check for cancellation when it is not suspended.
     * Use [yield] or [CoroutineScope.isActive] to periodically check for cancellation in tight loops if needed.
     *
     * Use [tryAcquire] to try acquire a permit of this semaphore without suspension.
     */
    public suspend fun acquire()

    /**
     * Tries to acquire a permit from this semaphore without suspension.
     *
     * @return `true` if a permit was acquired, `false` otherwise.
     */
    public fun tryAcquire(): Boolean

    /**
     * Releases a permit, returning it into this semaphore. Resumes the first
     * suspending acquirer if there is one at the point of invocation.
     */
    public fun release()
}

/**
 * Creates new [Semaphore] instance.
 */
@Suppress("FunctionName")
public fun Semaphore(maxPermits: Int, acquiredPermits: Int = 0): Semaphore = SemaphoreImpl(maxPermits, acquiredPermits)

/**
 * Executes the given [action] with acquiring a permit from this semaphore at the beginning
 * and releasing it after the [action] is completed.
 *
 * @return the return value of the [action].
 */
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