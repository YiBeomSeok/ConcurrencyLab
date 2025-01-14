package pipeline

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

// p3의 작업 시간이 전체 작업 시간에 영향을 끼치고 있다.
// 이번에는 총 작업 시간이 30초 이하인 동안에만 프로세싱을 한다.
// p3의 작업 시간이 44ms인 경우
// 900장을 원하지만 677장이 처리되었다.
// p3의 작업 시간이 31ms인 경우
// 900장을 원하지만 927장이 처리되었다.
class Pipeline3 {
    suspend fun p1(frame: Frame): Frame {
        delay(10) // 10ms 작업 시간
        if (Random.nextDouble(0.0, 1.0) < 0.0001) { // 0.01% 확률로 예외 발생
            throw IllegalStateException("[p1] Condition failed for id: ${frame.id}")
        }
        return frame
    }

    suspend fun p2(frame: Frame): Frame {
        delay(20) // 20ms 작업 시간
        if (Random.nextDouble(0.0, 1.0) < 0.0001) { // 0.01% 확률로 예외 발생
            throw IllegalStateException("[p2] Condition failed for id: ${frame.id}")
        }
        return frame
    }

    suspend fun p3(frame: Frame): Frame {
        delay(44) // 44ms 작업 시간
        if (Random.nextDouble(0.0, 1.0) < 0.001) { // 0.1% 확률로 예외 발생
            throw IllegalStateException("[p3] Condition failed for id: ${frame.id}")
        }
        return frame
    }
}

fun main() = runBlocking {
    val currentProcessingSecond = MutableStateFlow(0)

    val alarm = launch {
        while (isActive) {
            delay(1000)
            val t = currentProcessingSecond.updateAndGet { it + 1 }
            println(t)
        }
    }

    val pipeline = Pipeline3()

    // 30fps * 30초
    val bufferSize = 30 * 30

    // frames Flow 정의
    val frames = flow {
        var id = 0
        while (true) {
            delay(30)
            emit(Frame(id = ++id, "data"))
        }
    }

    while (true) {
        val processStart = System.currentTimeMillis()

        val result = runCatching {
            frames
                .buffer(bufferSize)
                .map { pipeline.p1(it) }
                .buffer(bufferSize)
                .map { pipeline.p2(it) }
                .buffer(bufferSize)
                .map { pipeline.p3(it) }
                .takeWhile {
                    (System.currentTimeMillis() - processStart).milliseconds.inWholeSeconds <= 30
                } // 30초에 해당하는 작업물들만 취합
                .toList()
        }
            .onFailure { e ->
                println("에러 발생. 처음부터 다시 시도합니다. 에러: ${e.message}")
                currentProcessingSecond.update { 0 }
            }
            .getOrNull()

        if (result != null) {
            val processEnd = System.currentTimeMillis()
            println("Processing Time : ${(processEnd - processStart)} ms")
            println("모든 프레임 처리 완료! 총 프레임: ${result.size}")
            break // 성공적으로 처리했으므로 반복 종료
        }
    }

    alarm.cancel()
}