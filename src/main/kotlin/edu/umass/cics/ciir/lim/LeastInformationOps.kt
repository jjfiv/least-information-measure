package edu.umass.cics.ciir.lim

import gnu.trove.map.hash.TLongDoubleHashMap
import org.lemurproject.galago.core.retrieval.RequiredParameters
import org.lemurproject.galago.core.retrieval.RequiredStatistics
import org.lemurproject.galago.core.retrieval.iterator.*
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode
import org.lemurproject.galago.core.retrieval.query.NodeParameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.InputStreamReader

class PlainSumIterator(np: NodeParameters, val qiters: Array<ScoreIterator>) : DisjunctionIterator(qiters), ScoreIterator {
    override fun maximumScore(): Double {
        var sum = 0.0;
        qiters.forEach { sum += it.maximumScore() }
        return sum
    }

    override fun minimumScore(): Double {
        var sum = 0.0;
        qiters.forEach { sum += it.minimumScore() }
        return sum
    }

    override fun getAnnotatedNode(sc: ScoringContext?): AnnotatedNode {
        throw RuntimeException()
    }

    override fun getValueString(sc: ScoringContext?): String {
        return "${this.currentCandidate()} ${this.score(sc)}";
    }

    override fun score(c: ScoringContext?): Double {
        var sum = 0.0;
        qiters.forEach { sum += it.score(c) }
        return sum
    }
}

class NormSumIterator(np: NodeParameters, val qiters: Array<ScoreIterator>) : DisjunctionIterator(qiters), ScoreIterator {
    companion object {
        val norms = TLongDoubleHashMap()
        init {
            InputStreamReader(StreamCreator.openInputStream("norms.tsv.gz")).useLines { lines ->

                lines.forEach { line ->
                    val cols = line.split("\t")
                    val docNo = cols[0].toLongOrNull() ?: error("docNo not long")
                    norms.put(docNo, cols[1].toDoubleOrNull() ?: error("Norm not double"))
                }
            }
        }
    }
    override fun maximumScore(): Double {
        var sum = 0.0;
        qiters.forEach { sum += it.maximumScore() }
        return sum
    }

    override fun minimumScore(): Double {
        var sum = 0.0;
        qiters.forEach { sum += it.minimumScore() }
        return sum
    }

    override fun getAnnotatedNode(sc: ScoringContext?): AnnotatedNode {
        throw RuntimeException()
    }

    override fun getValueString(sc: ScoringContext?): String {
        return "${this.currentCandidate()} ${this.score(sc)}";
    }

    override fun score(c: ScoringContext?): Double {
        val norm = norms[c!!.document]
        var sum = 0.0;
        qiters.forEach { sum += it.score(c) }
        return sum / norm
    }

}


@RequiredStatistics(statistics = arrayOf("collectionLength", "nodeFrequency", "documentCount", "nodeDocumentCount"))
class LeastInformationBinary(np: NodeParameters, lengths: LengthsIterator, counts: CountIterator) : ScoringFunctionIterator(np, lengths, counts) {

    // Total term occurrences in collection...
    val collectionLength = np.getLong("collectionLength")
    // Total term occurrences.
    val collectionFrequency = np.getLong("nodeFrequency")
    // Total docs in collection
    val N = np.getLong("documentCount")
    // n_i
    val df = np.getLong("nodeDocumentCount")

    val gtC = g(df.toDouble() / N.toDouble())

    // This is the core function of LIB: http://www.wolframalpha.com/input/?i=x*(1+-+ln(x))+from+x+%3D+0+to+x+%3D+1
    fun g(p: Double) = p * (1 - Math.log(p))

    override fun score(c: ScoringContext?): Double {
        val count = this.countIterator.count(c)
        if (count > 0) {
            return 1.0 - gtC;
        } else {
            return -gtC;
        }
    }

    override fun hasMatch(context: ScoringContext?): Boolean {
        // hack to verify that negative scores are not useful.
        return super.hasMatch(context)
    }
}

@RequiredStatistics(statistics = arrayOf("collectionLength", "nodeFrequency", "documentCount", "nodeDocumentCount"))
class LeastInformationFrequency(np: NodeParameters, lengths: LengthsIterator, counts: CountIterator) : ScoringFunctionIterator(np, lengths, counts) {

    // Total term occurrences in collection...
    val collectionLength = np.getLong("collectionLength")
    // Total term occurrences.
    val collectionFrequency = np.getLong("nodeFrequency")

    val gtC = g(collectionFrequency.toDouble() / collectionLength.toDouble())

    // This is the core function of LIB: http://www.wolframalpha.com/input/?i=x*(1+-+ln(x))+from+x+%3D+0+to+x+%3D+1
    fun g(p: Double): Double = p * (1 - Math.log(p))

    override fun score(c: ScoringContext?): Double {
        val count = this.countIterator.count(c)
        if (count == 0) {
            return -gtC;
        }
        val length = this.lengthsIterator.length(c)
        val tfd = count.toDouble() / length.toDouble()
        val gtd = g(tfd);
        return gtd - gtC;
    }

    override fun hasMatch(context: ScoringContext?): Boolean {
        // hack to verify that negative scores are not useful.
        //return true
        return super.hasMatch(context)
    }
}

@RequiredStatistics(statistics = arrayOf("collectionLength", "nodeFrequency", "documentCount", "nodeDocumentCount"))
class LIBplusLIF(np: NodeParameters, lengths: LengthsIterator, counts: CountIterator) : ScoringFunctionIterator(np, lengths, counts) {

    // Total term occurrences in collection...
    val collectionLength = np.getLong("collectionLength")
    // Total term occurrences.
    val collectionFrequency = np.getLong("nodeFrequency")

    // Total docs in collection
    val N = np.getLong("documentCount")
    // n_i
    val df = np.getLong("nodeDocumentCount")

    val lib_gtC = g(df.toDouble() / N.toDouble())
    val lif_gtC = g(collectionFrequency.toDouble() / collectionLength.toDouble())

    val bg = (-lib_gtC) + (-lif_gtC);

    // This is the core function of LIB: http://www.wolframalpha.com/input/?i=x*(1+-+ln(x))+from+x+%3D+0+to+x+%3D+1
    fun g(p: Double): Double = p * (1 - Math.log(p))

    override fun score(c: ScoringContext?): Double {
        val count = this.countIterator.count(c)
        if (count == 0) {
            return bg
        }
        val length = this.lengthsIterator.length(c)
        val tfd = count.toDouble() / length.toDouble()
        val gtd = g(tfd);
        val lif = gtd -lif_gtC;
        val lib = 1.0 - lib_gtC

        return lif + lib;
    }

    override fun hasMatch(context: ScoringContext?): Boolean {
        // hack to verify that negative scores are not useful.
        //return true
        return super.hasMatch(context)
    }
}

@RequiredStatistics(statistics = arrayOf("collectionLength", "nodeFrequency", "documentCount", "nodeDocumentCount"))
class LIBtimesLIF(np: NodeParameters, lengths: LengthsIterator, counts: CountIterator) : ScoringFunctionIterator(np, lengths, counts) {

    // Total term occurrences in collection...
    val collectionLength = np.getLong("collectionLength")
    // Total term occurrences.
    val collectionFrequency = np.getLong("nodeFrequency")

    // Total docs in collection
    val N = np.getLong("documentCount")
    // n_i
    val df = np.getLong("nodeDocumentCount")

    val lib_gtC = g(df.toDouble() / N.toDouble())
    val lif_gtC = g(collectionFrequency.toDouble() / collectionLength.toDouble())

    val bg = (1.0-lib_gtC) * (1.0-lif_gtC);

    // This is the core function of LIB: http://www.wolframalpha.com/input/?i=x*(1+-+ln(x))+from+x+%3D+0+to+x+%3D+1
    fun g(p: Double): Double = p * (1 - Math.log(p))

    override fun score(c: ScoringContext?): Double {
        val count = this.countIterator.count(c)
        if (count == 0) {
            return bg
        }
        val length = this.lengthsIterator.length(c)
        val tfd = count.toDouble() / length.toDouble()
        val gtd = g(tfd);
        val lif = 1.0 + gtd -lif_gtC;
        val lib = 2.0 - lib_gtC

        return lif * lib;
    }

    override fun hasMatch(context: ScoringContext?): Boolean {
        // hack to verify that negative scores are not useful.
        //return true
        return super.hasMatch(context)
    }
}

@RequiredStatistics(statistics = arrayOf("collectionLength", "nodeFrequency", "documentCount", "nodeDocumentCount"))
@RequiredParameters(parameters=arrayOf("mu"))
class DirichletLeastInformationFrequency(np: NodeParameters, lengths: LengthsIterator, counts: CountIterator) : ScoringFunctionIterator(np, lengths, counts) {
    val mu = np.get("mu", 1500.0);
    // Total term occurrences in collection...
    val collectionLength = np.getLong("collectionLength")
    // Total term occurrences.
    val collectionFrequency = np.getLong("nodeFrequency")
    val background = Math.max(collectionFrequency.toDouble(), 0.5) / collectionLength.toDouble()

    val gtC = g(collectionFrequency.toDouble() / collectionLength.toDouble())

    // This is the core function of LIB: http://www.wolframalpha.com/input/?i=x*(1+-+ln(x))+from+x+%3D+0+to+x+%3D+1
    fun g(p: Double): Double = p * (1 - Math.log(p))

    override fun score(c: ScoringContext?): Double {
        val count = this.countIterator.count(c)
        if (count == 0) {
            return -gtC;
        }
        val length = this.lengthsIterator.length(c)
        val tfd = (count.toDouble() + mu*background) / (length.toDouble() + mu)
        val gtd = g(tfd);
        return gtd - gtC;
    }

    override fun hasMatch(context: ScoringContext?): Boolean {
        // hack to verify that negative scores are not useful.
        //return true
        return super.hasMatch(context)
    }
}
