package edu.umass.cics.ciir.lim

import com.github.benmanes.caffeine.cache.Caffeine
import gnu.trove.list.array.TDoubleArrayList
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.index.corpus.CorpusReader
import org.lemurproject.galago.core.index.stats.NodeStatistics
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.utility.StreamCreator
import java.io.PrintWriter
import java.time.Duration

/**
 *
 * @author jfoley.
 */
class Debouncer
/**
 * @param milliseconds the minimum number of milliseconds between actions.
 */
@JvmOverloads constructor(var delay: Long = 1000) {
    var startTime: Long = 0
    var lastTime: Long = 0

    init {
        this.startTime = System.currentTimeMillis()
        this.lastTime = startTime - delay
    }

    fun ready(): Boolean {
        val now = System.currentTimeMillis()
        if (now - lastTime >= delay) {
            lastTime = now
            return true
        }
        return false
    }

    fun estimate(currentItem: Long, totalItems: Long): RateEstimate {
        return RateEstimate(System.currentTimeMillis() - startTime, currentItem, totalItems)
    }

    class RateEstimate(
            /** time spent on job (s)  */
            private val time: Long, private val itemsComplete: Long, private val totalItems: Long) {
        /** fraction of job complete  */
        private val fraction: Double = itemsComplete / totalItems.toDouble()

        /** items/ms  */
        private fun getRate(): Double = itemsComplete / time.toDouble()
        /** estimated time remaining (ms)  */
        private val remaining: Double

        init {
            this.remaining = (totalItems - itemsComplete) / getRate()
        }

        fun itemsPerSecond(): Double {
            return getRate() * 1000.0
        }

        fun percentComplete(): Double {
            return fraction * 100.0
        }

        override fun toString(): String {
            return String.format("%d/%d items, %4.1f items/s [%s left]; [%s spent], %2.1f%% complete.", itemsComplete, totalItems, itemsPerSecond(), Debouncer.prettyTimeOfMillis(remaining.toLong()), Debouncer.prettyTimeOfMillis(time), percentComplete())
        }
    }

    companion object {

        fun prettyTimeOfMillis(millis: Long): String {
            val msg = StringBuilder()
            var dur = Duration.ofMillis(millis)
            val days = dur.toDays()
            if (days > 0) {
                dur = dur.minusDays(days)
                msg.append(days).append(" days")
            }
            val hours = dur.toHours()
            if (hours > 0) {
                dur = dur.minusHours(hours)
                if (msg.length > 0) msg.append(", ")
                msg.append(hours).append(" hours")
            }
            val minutes = dur.toMinutes()
            if (minutes > 0) {
                if (msg.length > 0) msg.append(", ")
                msg.append(minutes).append(" minutes")
            }
            dur = dur.minusMinutes(minutes)
            val seconds = dur.toMillis() / 1e3
            if (msg.length > 0) msg.append(", ")
            msg.append(String.format("%1.3f", seconds)).append(" seconds")
            return msg.toString()
        }
    }
}

// In order to power LICos, we need to know the \sqrt(sum_t lib+lif(t, D)) for every document.
object GenerateNorms {
    fun g(p: Double): Double = p * (1 - Math.log(p))
    @JvmStatic fun main(args: Array<String>) {
        val justTerms = Document.DocumentComponents.JustTerms

        val statsCache = Caffeine.newBuilder().maximumSize(200_000).build<String, NodeStatistics>()

        DataPaths.getRobustIndex().use { ret ->

            val cstats = ret.getCollectionStatistics(GExpr("lengths"))!!
            val total = cstats.documentCount
            val totalTerms = cstats.collectionLength
            var processed = 0L;
            val msg = Debouncer()

            val corpus = (ret.index.getIndexPart("corpus") as CorpusReader)
            val iter = corpus.iterator
            PrintWriter(StreamCreator.openOutputStream("norms.tsv.gz")).use { out ->
                while(!iter.isDone) {
                    processed++

                    if (msg.ready()) {
                        println(msg.estimate(processed, total))
                    }
                    val tfv = iter.getDocument(justTerms).terms!!
                    val length = tfv.size
                    val freqs = TObjectIntHashMap<String>()
                    tfv.forEach { freqs.adjustOrPutValue(it, 1, 1) }

                    val stats = HashMap<String, NodeStatistics>()
                    freqs.forEachKey { term ->
                        stats.put(term, statsCache.get(term, {missing -> ret.getNodeStatistics(GExpr("counts", missing))}) ?: NodeStatistics())
                        true
                    }

                    val NDF = total.toDouble()
                    val NCF = totalTerms.toDouble()
                    val DL = length.toDouble()
                    val vector = TDoubleArrayList()
                    freqs.forEachEntry {term, count ->
                        val nodeStats = stats[term]!!
                        val lifp = nodeStats.nodeFrequency.toDouble() / NDF
                        val libp = nodeStats.nodeDocumentCount.toDouble() / NCF

                        val lib_gtC = g(libp)
                        val lif_gtC = g(lifp)
                        val lib = 1.0 - lib_gtC
                        val lif = g(count.toDouble() / DL) - lif_gtC
                        vector.add(lib+lif)
                        true
                    }

                    out.println("${iter.keyString}\t${norm(vector)}")

                    if (!iter.nextKey()) break
                }
            }
        }
    }

    fun norm(xs: TDoubleArrayList): Double {
        var sumSq = 0.0
        (0 until xs.size()).forEach { i ->
            val xi = xs.getQuick(i)
            sumSq += xi*xi
        }
        return Math.sqrt(sumSq)
    }

}
