package edu.umass.cics.ciir.lim

import org.lemurproject.galago.core.retrieval.RequiredStatistics
import org.lemurproject.galago.core.retrieval.iterator.CountIterator
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.core.retrieval.query.NodeParameters

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
