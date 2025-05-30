package pipeline

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.milliseconds

class Pipeline1 {
    suspend fun p1(frame: Frame): Frame {
        delay(10)
        return frame
    }

    suspend fun p2(frame: Frame): Frame {
        delay(20)
        return frame
    }

    suspend fun p3(frame: Frame): Frame {
        delay(30)
        return frame
    }
}

fun main() = runBlocking {
    val alarm = launch {
        var sec = 1
        while (isActive) {
            delay(1000)
            println(sec++)
        }
    }

    val frames = flow {
        var id = 0
        while (true) {
            delay(30)
            emit(Frame(id = ++id, "data"))
        }
    }
    val recordingTime = 30
    val fps = 30
    val bufferSize = recordingTime * fps

    val processStart = System.currentTimeMillis()

    val pipeline = Pipeline1()

    frames
        .buffer(bufferSize)
        .map { frame ->
            pipeline.p1(frame)
        }
        .buffer(bufferSize)
        .map { frame ->
            pipeline.p2(frame)
        }
        .buffer(bufferSize)
        .map { frame ->
            pipeline.p3(frame)
        }
        .take(bufferSize)
        .toList()

    val processEnd = System.currentTimeMillis()

    println("Processing Time : ${(processEnd - processStart).milliseconds.inWholeSeconds}")
    alarm.cancel()
}