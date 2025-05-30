package compare

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import java.util.concurrent.PriorityBlockingQueue
import kotlin.system.measureTimeMillis

data class Frame(val id: Int, val data: String = "data")

suspend fun p1(frame: Frame): Frame {
    delay(60); return frame
}

suspend fun p2(frame: Frame): Frame {
    delay(30); return frame
}

suspend fun p3(frame: Frame): Frame {
    delay(30); return frame
}

class SequentialPipeline(private val total: Int) {

    suspend fun run(frames: Flow<Frame>): List<Frame> {
        return frames
            .buffer(capacity = total)
            .map { p1(it) }
            .buffer(capacity = total)
            .map { p2(it) }
            .buffer(capacity = total)
            .map { p3(it) }
            .take(total)
            .toList()
    }
}

class OrderedPipeline(private val workers: Int = 4, private val total: Int) {
    private data class Tagged<T>(val idx: Long, val value: T)

    private fun <T> Flow<Tagged<T>>.ordered(): Flow<Tagged<T>> = flow {
        val heap = PriorityBlockingQueue<Tagged<T>>(11) { a, b -> (a.idx - b.idx).toInt() }
        var nextIdx = 0L
        collect { tagged ->
            heap.add(tagged)
            while (true) {
                val next = heap.peek() ?: break
                if (next.idx != nextIdx) break
                emit(heap.poll())
                nextIdx++
            }
        }
    }

    suspend fun run(frames: Flow<Frame>): List<Frame> {
        // 인덱스 부여
        var index = 0L
        val source: Flow<Tagged<Frame>> = frames.map { frame ->
            Tagged(index++, frame)
        }

        val resultFlow = source
            .flatMapMerge(concurrency = workers) { tagged ->
                flow {
                    val out = p1(tagged.value)
                    emit(Tagged(tagged.idx, out))
                }.flowOn(Dispatchers.Default)
            }
            .ordered()
            .flatMapMerge(concurrency = workers) { tagged ->
                flow {
                    val out = p2(tagged.value)
                    emit(Tagged(tagged.idx, out))
                }.flowOn(Dispatchers.Default)
            }
            .ordered()
            .flatMapMerge(concurrency = workers) { tagged ->
                flow {
                    val out = p3(tagged.value)
                    emit(Tagged(tagged.idx, out))
                }.flowOn(Dispatchers.Default)
            }
            .ordered()

        return resultFlow
            .map { it.value }
            .take(total)
            .toList()
    }
}

fun main() = runBlocking {
    val frameIntervalMs = 30L
    val TOTAL_FRAMES = 30 * 5

    // frames Flow (30fps)
    val frames: Flow<Frame> = flow {
        var id = 0
        while (true) {
            delay(frameIntervalMs)
            emit(Frame(id = ++id))
        }
    }

    println("Starting Sequential Pipeline...")
    val seqPipeline = SequentialPipeline(TOTAL_FRAMES)
    val seqTime = measureTimeMillis {
        val seqResult = seqPipeline.run(frames)
        println("Sequential processed frames: ${seqResult.size}")
    }
    println("Sequential elapsed: $seqTime ms\n")

    println("Starting Ordered Pipeline...")
    val ordPipeline = OrderedPipeline(workers = 4, total = TOTAL_FRAMES)
    val ordTime = measureTimeMillis {
        val ordResult = ordPipeline.run(frames)
        println("Ordered processed frames: ${ordResult.size}")
    }
    println("Ordered elapsed: $ordTime ms")
}
