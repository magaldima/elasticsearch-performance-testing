package com.bfm.aw.esloadgenerator

import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.ByteSizeUnit
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.threadpool.ThreadPool
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import java.lang.Integer.parseInt
import java.lang.Long.parseLong
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer


@SpringBootApplication
class EsLoadGeneratorApplication {

    val numRows = parseInt(System.getProperty("numRows", "10000"))

    val executionStartTimes = ConcurrentHashMap<Long, Long>()
    val dpsByNumColumns = ConcurrentHashMap<Int, Double>()

    @Bean
    fun restClient(): RestClient {
        val port = parseInt(System.getProperty("port", "9200"))
        val hosts = System.getProperty("hosts", "127.0.0.1").split(",").map { host -> HttpHost(host, port) }.toTypedArray()
        return RestClient
                .builder(*hosts)
                .build()
    }

    @Bean
    fun restHighLevelClient(restClient: RestClient): RestHighLevelClient {
        return RestHighLevelClient(restClient)
    }

    @Bean
    fun cliRunner(restHighLevelClient: RestHighLevelClient): CommandLineRunner {
        return CommandLineRunner { _ ->
            run {
                val numColumns = System.getProperty("numColumns", "400,200,100,50,10").split(",")
                numColumns.forEach {
                    runBulkProcessor(restHighLevelClient, parseInt(it))
                }
                println(dpsByNumColumns)
                System.exit(0)
            }
        }
    }

    private fun runBulkProcessor(client: RestHighLevelClient, numColumns: Int) {
        println("numColumns = $numColumns, numRows=$numRows")
        val totalDocs = AtomicLong(0)
        val totalTime = AtomicLong(0)
        val threadPool = ThreadPool(Settings.EMPTY)
        val listener = object : BulkProcessor.Listener {

            override fun beforeBulk(executionId: Long, request: BulkRequest) {
                executionStartTimes.put(executionId, System.currentTimeMillis())
                println("Requesting - executionId: $executionId, size: ${request.estimatedSizeInBytes()} bytes")
            }

            override fun afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse) {
                val timeElapsed = System.currentTimeMillis() - executionStartTimes.get(executionId)!!
                val numActions = request.numberOfActions()
                val cumulativeTime = totalTime.addAndGet(timeElapsed)
                val cumulativeDocs = totalDocs.addAndGet(numActions.toLong())
                println("Response - executionId: $executionId, request: $response, execution time: $timeElapsed ms, # items: $numActions")
                println("Cumulative Docs: $cumulativeDocs, cumulative time: $cumulativeTime, cumulative dps: ${(cumulativeDocs / cumulativeTime.toDouble()) * 1000}")
                executionStartTimes.remove(executionId)
            }

            override fun afterBulk(executionId: Long, request: BulkRequest, failure: Throwable) {
            }
        }
        val bulkProcessor = BulkProcessor
                .Builder(BiConsumer { t, u -> client.bulkAsync(t, u) }, listener, threadPool)
                .setBulkSize(ByteSizeValue(parseLong(System.getProperty("batchSize", "10")), ByteSizeUnit.MB))
                .build()
        // run numRows index requests
        val rand = Random()
        (1..numRows).forEach { _ ->
            val doc = (1..numColumns).map { i -> "column-$i" to rand.nextGaussian() }.toMap()
            bulkProcessor.add(
                    IndexRequest("test", "main").source(doc, XContentType.JSON)
            )
        }
        bulkProcessor.flush()
        /*
        wait until all outstanding requests finish
         */
        while (true) {
            if (executionStartTimes.isEmpty()) {
                val dps = (totalDocs.get() / totalTime.get().toDouble()) * 1000
                println("Final # items: ${totalDocs.get()}, final time: ${totalTime.get()} ms ,final dps: $dps")
                dpsByNumColumns.put(numColumns, dps)
                break
            }
            Thread.sleep(500)
        }
    }

}

fun main(args: Array<String>) {
    SpringApplication.run(EsLoadGeneratorApplication::class.java, *args)
}
