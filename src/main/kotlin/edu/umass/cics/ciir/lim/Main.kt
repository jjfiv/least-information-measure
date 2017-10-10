package edu.umass.cics.ciir.lim

import gnu.trove.list.array.TDoubleArrayList
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.core.eval.metric.QueryEvaluatorFactory
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.net.InetAddress
import java.util.*

typealias GExpr = org.lemurproject.galago.core.retrieval.query.Node

object DataPaths {
    val host: String = InetAddress.getLocalHost().hostName
    fun notImpl(): Nothing = throw RuntimeException("notImpl")
    fun getRobustIndex(): LocalRetrieval = LocalRetrieval(when(host) {
        "gob" -> "/media/jfoley/flash/robust04.galago"
        else -> notImpl()
    })
    fun getQueryDir(): File = File(when(host) {
        "gob" -> "/home/jfoley/code/queries/robust04/"
        else -> notImpl()
    })
    fun getTitleQueryFile(): File = File(getQueryDir(), "rob04.titles.tsv")
    fun getDescQueryFile(): File = File(getQueryDir(), "rob04.desc.tsv")
    fun getQueryJudgmentsFile(): File = File(getQueryDir(), "robust04.qrels")

    fun parseTSV(fp: File): Map<String, String> = fp.useLines { lines ->
        lines.associate { line ->
            val cols = line.trim().split("\t")
            assert(cols.size == 2)
            Pair(cols[0], cols[1])
        }
    }
    fun getTitleQueries(): Map<String, String> = parseTSV(getTitleQueryFile())
    fun getDescQueries(): Map<String, String> = parseTSV(getDescQueryFile())
    fun getQueryJudgments(): QuerySetJudgments = QuerySetJudgments(getQueryJudgmentsFile().absolutePath, false, false);
}

fun GExpr.push(what: GExpr): GExpr {
    this.addChild(what)
    return this
}

class NamedMeasures {
    val measures = HashMap<String, TDoubleArrayList>()
    fun push(what: String, x: Double) {
        measures.computeIfAbsent(what, {TDoubleArrayList()}).add(x)
    }
    fun means(): TreeMap<String, Double> = measures.mapValuesTo(TreeMap()) { (_,arr) -> arr.sum() / arr.size() }
}

fun main(args: Array<String>) {
    DataPaths.getRobustIndex().use { ret ->
        val tokenizer = ret.tokenizer!!

        val tqs = DataPaths.getTitleQueries()
        val qrels = DataPaths.getQueryJudgments()
        val AP = QueryEvaluatorFactory.create("ap", Parameters.create())!!
        val measures = NamedMeasures()

        tqs.forEach { qid, qtext ->
            println(qid)
            val qterms = tokenizer.tokenize(qtext).terms.filterNotNull()
            val truth = qrels.get(qid)!!

            val qlExpr = GExpr("combine")
            qterms.forEach {
                qlExpr.push(GExpr.Text(it))
            }

            val results = ret.transformAndExecuteQuery(qlExpr)!!
            val score = AP.evaluate(QueryResults(results.scoredDocuments), truth)
            measures.push("ql-ap", score)
            println(measures.means())
        }
        println(measures.means())

        //ret.index.getIndexPart()
        //println(index..toPrettyString())
    }

}