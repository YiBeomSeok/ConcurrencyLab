package order

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import java.util.concurrent.PriorityBlockingQueue
import kotlin.system.measureTimeMillis

data class Tagged<T>(val idx: Long, val value: T)

fun <T> Flow<Tagged<T>>.ordered(): Flow<Tagged<T>> = flow {
    val heap = PriorityBlockingQueue<Tagged<T>>(11) { a, b -> (a.idx - b.idx).toInt() }
    var nextIdx = 0L
    collect { tagged ->
        heap.add(tagged)
        while (heap.peek()?.idx == nextIdx) {
            emit(heap.poll())
            nextIdx++
        }
    }
}

suspend fun process1(x: Int): Int {
    delay(30)
    return x + 1
}

suspend fun process2(x: Int): Int {
    delay(30)
    return x * 2
}

suspend fun process3(x: Int): Int {
    delay(30)
    return x - 1
}

suspend fun process4(x: Int): Int {
    delay(30)
    return x * 10
}

fun main() = runBlocking {
    val TOTAL = 20L
    val WORKERS = listOf(3, 3, 3, 3)

    val elapsed = measureTimeMillis {
        val source: Flow<Tagged<Int>> = flow {
            repeat(TOTAL.toInt()) { i ->
                emit(Tagged(i.toLong(), i))
            }
        }
            .buffer(capacity = 50, onBufferOverflow = BufferOverflow.DROP_OLDEST)
            .flowOn(Dispatchers.IO)

        // 5) 4단계 파이프라인 구성
        val finalFlow: Flow<Tagged<Int>> = source
            .flatMapMerge(concurrency = WORKERS[0]) { tagged ->
                flow {
                    val out1 = process1(tagged.value)
                    emit(Tagged(tagged.idx, out1))
                }.flowOn(Dispatchers.Default)
            }
            .ordered()
            .flatMapMerge(concurrency = WORKERS[1]) { tagged ->
                flow {
                    val out2 = process2(tagged.value)
                    emit(Tagged(tagged.idx, out2))
                }.flowOn(Dispatchers.Default)
            }
            .ordered()
            .flatMapMerge(concurrency = WORKERS[2]) { tagged ->
                flow {
                    val out3 = process3(tagged.value)
                    emit(Tagged(tagged.idx, out3))
                }.flowOn(Dispatchers.Default)
            }
            .ordered()
            .flatMapMerge(concurrency = WORKERS[3]) { tagged ->
                flow {
                    val out4 = process4(tagged.value)
                    emit(Tagged(tagged.idx, out4))
                }.flowOn(Dispatchers.Default)
            }
            .ordered()

        val results: List<Int> = finalFlow
            .map { it.value }
            .toList()

        println("[Final Results] $results")
    }

    val predicted = 4 * 30 + (TOTAL - 1) * 30 / WORKERS.max()
    println("Elapsed time: $elapsed ms, Predicted time: $predicted ms")
}