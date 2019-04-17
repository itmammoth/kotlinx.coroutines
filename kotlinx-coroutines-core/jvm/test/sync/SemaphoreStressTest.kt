package kotlinx.coroutines.sync

import kotlinx.coroutines.TestBase
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import org.junit.Test
import kotlin.test.assertEquals

class SemaphoreStressTest : TestBase() {

    @Test
    fun stressTestAsMutex() = runTest {
        val n = 1000 * stressTestMultiplier
        val k = 100
        var shared = 0
        val semaphore = Semaphore(1)
        val jobs = List(n) {
            launch {
                repeat(k) {
                    semaphore.acquire()
                    shared++
                    semaphore.release()
                }
            }
        }
        jobs.forEach { it.join() }
        assertEquals(n * k, shared)
    }

    @Test
    fun stressTest() = runTest {
        val n = 10000 * stressTestMultiplier
        val k = 100
        val semaphore = Semaphore(10)
        val jobs = List(n) {
            launch {
                repeat(k) {
                    semaphore.acquire()
                    semaphore.release()
                }
            }
        }
        jobs.forEach { it.join() }
    }

    @Test
    fun stressCancellation() = runTest {
        val n = 100_000 * stressTestMultiplier
        val semaphore = Semaphore(1)
        semaphore.acquire()
        repeat(n) {
            val job = launch {
                semaphore.acquire()
            }
            yield()
            job.cancelAndJoin()
        }
        assertEquals(0, semaphore.availablePermits)
        semaphore.release()
        assertEquals(1, semaphore.availablePermits)
    }
}