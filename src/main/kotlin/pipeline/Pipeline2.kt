package pipeline

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.random.Random

// p3의 작업 시간이 총 시간에 영향을 끼치고 있다.
// 만약 p3의 작업을 더 나눠서 각각의 작업이 30ms 이하가 된다면 총 작업시간을 다시 30초로 맞출 수 있다.
class Pipeline2 {
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
    val alarm = launch {
        var sec = 1
        while(isActive) {
            delay(1000)
            println(sec++)
        }
    }

    val pipeline = Pipeline2()

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

    while(true) {
        val processStart = System.currentTimeMillis()

        val result = runCatching {
            frames
                .buffer(bufferSize)
                .map { pipeline.p1(it) }
                .buffer(bufferSize)
                .map { pipeline.p2(it) }
                .buffer(bufferSize)
                .map { pipeline.p3(it) }
                .take(bufferSize)       // 900장 처리
                .toList()               // 최종 컬렉션으로 방출
        }

        val processEnd = System.currentTimeMillis()
        println("Processing Time : ${(processEnd - processStart)} ms")

        if (result.isSuccess) {
            println("모든 프레임 처리 완료!")
            break // 성공적으로 처리했으므로 반복 종료
        } else {
            println("에러 발생. 처음부터 다시 시도합니다. 에러: ${result.exceptionOrNull()?.message}")
            // 필요하면 delay(...) 등으로 재시작 간격 둠
        }
    }

    alarm.cancel()
}