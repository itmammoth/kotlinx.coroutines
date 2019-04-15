package kotlinx.coroutines.sync

import kotlinx.atomicfu.*
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.internal.SharedImmutable
import kotlinx.coroutines.internal.Symbol
import kotlinx.coroutines.internal.systemProp
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

public fun Semaphore(maxPermits: Int): Semaphore = SemaphoreImpl(maxPermits)

public suspend inline fun <T> Semaphore.withSemaphore(action: () -> T): T {
    acquire()
    try {
        return action()
    } finally {
        release()
    }
}

private class SemaphoreImpl(@JvmField val maxPermits: Int): Semaphore {
    init {
        require(maxPermits > 0) { "Semaphore should have at least 1 permit"}
    }

    private val _availablePermits = atomic(maxPermits)
    override val availablePermits: Int get() = max(_availablePermits.value.toInt(), 0)

    private val enqIdx = atomic(0L)
    private val deqIdx = atomic(0L)

    private val head: AtomicRef<Segment>
    private val tail: AtomicRef<Segment>

    init {
        val emptyNode = Segment(0, null)
        head = atomic(emptyNode)
        tail = atomic(emptyNode)
    }

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
            check(cur < maxPermits) { "Cannot" }
            cur + 1
        }
        if (p >= 0) return // no waiters
        resumeNextFromQueue()
    }

    private suspend fun addToQueueAndSuspend() = suspendAtomicCancellableCoroutine<Unit> sc@ { cont ->
        val tail = this.tail.value
        val enqIdx = enqIdx.getAndIncrement()
        val segment = findOrCreateSegment(enqIdx / SEGMENT_SIZE, tail)
        val i = (enqIdx % SEGMENT_SIZE).toInt()
        if (segment === null || segment[i].value === RESUMED || !segment[i].compareAndSet(null, cont)) {
            cont.resume(Unit)
            return@sc
        }
        cont.invokeOnCancellation {
            segment.clean(i)
            release()
        }
    }

    private fun resumeNextFromQueue() {
        val head = this.head.value
        val deqIdx = deqIdx.getAndIncrement()
        val segment = getHeadAndUpdate(deqIdx / SEGMENT_SIZE, head) ?: return
        val i = (deqIdx % SEGMENT_SIZE).toInt()
        val cont = segment[i].getAndUpdate {
            if (it === CLEANED) it else RESUMED
        }
        if (cont === CLEANED) return
        cont as CancellableContinuation<Unit>
        cont.resume(Unit)
    }

    /**
     * Finds or creates segment similarly to [findOrCreateSegment],
     * but updates the [head] reference to the found segment as well.
     */
    private fun getHeadAndUpdate(id: Long, headOrOutdated: Segment): Segment? {
        // Check whether the provided segment has the required `id`
        // and just return it in this case.
        if (headOrOutdated.id == id) {
            return headOrOutdated
        }
        // Find (or even create) the required segment
        // and update the `head` pointer.
        val head = findOrCreateSegment(id, headOrOutdated) ?: return null
        moveHeadForward(head)
        // We should clean `prev` references on `head` updates,
        // so they do not reference to the old segments. However,
        // it is fine to clean the `prev` reference of the new head only.
        // The previous "chain" of segments becomes no longer available from
        // segment queue structure and can be collected by GC.
        //
        // Note, that in practice it would be better to clean `next` references as well,
        // since it helps some GC (on JVM). However, this breaks the algorithm.
        head.prev.value = null
        return head
    }

    /**
     * Finds or creates a segment with the specified [id] if it exists,
     * or with a minimal but greater than the specified `id`
     * (`segment.id >= id`) if the required segment was removed
     * This method starts search from the provided [cur] segment,
     * going by `next` references. Returns `null` if this channels is closed
     * and a new segment should be added.
     */
    private fun findOrCreateSegment(id: Long, cur: Segment): Segment? {
        if (cur.id > id) return null
        // This method goes through `next` references and
        // adds new segments if needed, similarly to the `push` in
        // the Michael-Scott queue algorithm.
        var cur = cur
        while (cur.id < id) {
            var curNext = cur.next.value
            if (curNext == null) {
                // Add a new segment.
                val newTail = Segment(cur.id + 1, cur)
                curNext = if (cur.next.compareAndSet(null, newTail)) {
                    if (cur.removed) {
                        cur.remove()
                    }
                    moveTailForward(newTail)
                    newTail
                } else {
                    cur.next.value!!
                }
            }
            cur = curNext
        }
        return cur
    }

    /**
     * Updates [head] to the specified segment
     * if its `id` is greater.
     */
    private fun moveHeadForward(new: Segment) {
        while (true) {
            val cur = head.value
            if (cur.id > new.id) return
            if (this.head.compareAndSet(cur, new)) return
        }
    }

    /**
     * Updates [tail] to the specified segment
     * if its `id` is greater.
     */
    private fun moveTailForward(new: Segment) {
        while (true) {
            val cur = this.tail.value
            if (cur.id > new.id) return
            if (this.tail.compareAndSet(cur, new)) return
        }
    }

}

private class Segment(@JvmField val id: Long) {
    constructor(id: Long, prev: Segment?) : this(id) {
        this.prev.value = prev
    }

    // == Waiters Array ==
    private val waiters = atomicArrayOfNulls<Any?>(SEGMENT_SIZE)

    operator fun get(index: Int): AtomicRef<Any?> = waiters[index]

    // == Michael-Scott Queue + Fast Removing from the Middle ==

    // Pointer to the next segments, updates
    // similarly to the Michael-Scott queue algorithm.
    val next = atomic<Segment?>(null) // null (not set) | Segment | CLOSED
    // Pointer to the previous non-empty segment (can be null!),
    // updates lazily (see `remove()` function).
    val prev = atomic<Segment?>(null)
    // Number of cleaned waiters in this segment.
    private val cleaned = atomic(0)
    val removed get() = cleaned.value == SEGMENT_SIZE

    /**
     * Cleans the waiter located by the specified index in this segment.
     */
    fun clean(index: Int) {
        // Clean the specified waiter and
        // check if all node items are cleaned.
        waiters[index].value = CLEANED
        if (cleaned.incrementAndGet() < SEGMENT_SIZE) return
        // Remove this node
        remove()
    }

    /**
     * Removes this node from the waiting queue and cleans all references to it.
     */
    fun remove() {
        var next = this.next.value ?: return // tail can't be removed
        // Find the first non-removed node (tail is always non-removed)
        while (next.removed) {
            next = this.next.value ?: return
        }
        // Find the first non-removed `prev` and remove this node
        var prev = prev.value
        while (true) {
            if (prev == null) {
                next.prev.value = null
                return
            }
            if (prev.removed) {
                prev = prev.prev.value
                continue
            }
            next.movePrevToLeft(prev)
            prev.movePrevNextToRight(next)
            if (next.removed || !prev.removed) return
            prev = prev.prev.value
        }
    }

    /**
     * Update [Segment.next] pointer to the specified one if
     * the `id` of the specified segment is greater.
     */
    private fun movePrevNextToRight(next: Segment) {
        while (true) {
            val curNext = this.next.value as Segment
            if (next.id <= curNext.id) return
            if (this.next.compareAndSet(curNext, next)) return
        }
    }

    /**
     * Update [Segment.prev] pointer to the specified segment if
     * its `id` is lower.
     */
    private fun movePrevToLeft(prev: Segment) {
        while (true) {
            val curPrev = this.prev.value ?: return
            if (curPrev.id <= prev.id) return
            if (this.prev.compareAndSet(curPrev, prev)) return
        }
    }
}

@SharedImmutable
private val RESUMED = Symbol("RESUMED")
@SharedImmutable
private val CLEANED = Symbol("CLEANED")
@SharedImmutable
private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.semaphore.segmentSize", 32)