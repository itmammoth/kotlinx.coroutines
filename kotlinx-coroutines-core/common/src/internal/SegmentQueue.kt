package kotlinx.coroutines.internal

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

internal abstract class SegmentQueue<S: Segment<S>>(createFirstSegment: Boolean = true) {
    private val _head: AtomicRef<S?>
    protected val head: S? get() = _head.value

    private val _tail: AtomicRef<S?>
    protected val tail: S? get() = _tail.value

    init {
        val initialSegment = if (createFirstSegment) newSegment(0) else null
        _head = atomic(initialSegment)
        _tail = atomic(initialSegment)
    }

    abstract fun newSegment(id: Long, prev: S? = null): S

    protected fun getSegment(startFrom: S?, id: Long): S? {
        var startFrom = startFrom
        if (startFrom === null) {
            val firstSegment = newSegment(0)
            if (_head.compareAndSet(null, firstSegment))
                startFrom = firstSegment
            else {
                startFrom = head!!
            }
        }
        if (startFrom.id > id) return null
        // This method goes through `next` references and
        // adds new segments if needed, similarly to the `push` in
        // the Michael-Scott queue algorithm.
        var cur: S = startFrom
        while (cur.id < id) {
            var curNext = cur.next.value
            if (curNext == null) {
                // Add a new segment.
                val newTail = newSegment(cur.id + 1, cur)
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
        if (cur.id != id) return null
        return cur
    }

    protected fun getSegmentAndMoveHeadForward(startFrom: S?, id: Long): S? {
        if (startFrom !== null && startFrom.id == id) return startFrom
        val s = getSegment(startFrom, id) ?: return null
        moveHeadForward(s)
        return s
    }

    /**
     * Updates [head] to the specified segment
     * if its `id` is greater.
     */
    private fun moveHeadForward(new: S) {
        while (true) {
            val cur = head!!
            if (cur.id > new.id) return
            if (_head.compareAndSet(cur, new)) {
                new.prev.value = null
                return
            }
        }
    }

    /**
     * Updates [tail] to the specified segment
     * if its `id` is greater.
     */
    private fun moveTailForward(new: S) {
        while (true) {
            val cur = tail
            if (cur !== null && cur.id > new.id) return
            if (_tail.compareAndSet(cur, new)) return
        }
    }
}

internal abstract class Segment<S: Segment<S>>(val id: Long, prev: S?) {
    // Pointer to the next segments, updates
    // similarly to the Michael-Scott queue algorithm.
    val next = atomic<S?>(null) // null (not set) | Segment | CLOSED
    // Pointer to the previous non-empty segment (can be null!),
    // updates lazily (see `remove()` function).
    val prev = atomic<S?>(null)

    abstract val removed: Boolean

    init {
        this.prev.value = prev
    }

    /**
     * Removes this node from the waiting queue and cleans all references to it.
     */
    fun remove() {
        check(removed) { " The segment should be logically removed at first "}
        val next = this.next.value ?: return // tail can't be removed
        // Find the first non-removed node (tail is always non-removed)
        val prev = prev.value ?: return // head cannot be removed
        next.movePrevToLeft(prev)
        prev.movePrevNextToRight(next)
        if (prev.removed)
            prev.remove()
        if (next.removed)
            next.remove()

//        while (next.removed) {
//            next = next.next.value ?: return
//        }
//        // Find the first non-removed `prev` and remove this node
//        var prev = prev.value
//        while (true) {
//            if (prev === null) {
//                next.prev.value = null
//                return
//            }
//            if (prev.removed) {
//                prev = prev.prev.value
//                continue
//            }
//            next.movePrevToLeft(prev)
//            prev.movePrevNextToRight(next)
//            if (next.removed || !prev.removed) return
//            prev = prev.prev.value
//        }
    }

    /**
     * Update [Segment.next] pointer to the specified one if
     * the `id` of the specified segment is greater.
     */
    private fun movePrevNextToRight(next: S) {
        while (true) {
            val curNext = this.next.value as S
            if (next.id <= curNext.id) return
            if (this.next.compareAndSet(curNext, next)) return
        }
    }

    /**
     * Update [Segment.prev] pointer to the specified segment if
     * its `id` is lower.
     */
    private fun movePrevToLeft(prev: S) {
        while (true) {
            val curPrev = this.prev.value ?: return
            if (curPrev.id <= prev.id) return
            if (this.prev.compareAndSet(curPrev, prev)) return
        }
    }
}