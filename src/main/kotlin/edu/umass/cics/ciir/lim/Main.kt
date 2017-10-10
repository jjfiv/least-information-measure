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
    fun getRobustIndex(): LocalRetrieval = getRobustIndex(Parameters.create())
    fun getRobustIndex(p: Parameters): LocalRetrieval = LocalRetrieval(when(host) {
        "gob" -> "/media/jfoley/flash/robust04.galago"
        else -> notImpl()
    }, p)
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


// Results:
//  hasMatch wired to true:
// { "lib-ap" : 0.13812366209817234 , "lif+lib-ap" : 0.2123746827490559 , "lif-ap" : 0.05467584931916471 , "ql-ap" : 0.251723934368579 }
// hasMatch smarter:
// { "lib-ap" : 0.13812366209817234 , "lif+lib-ap" : 0.2123746827490559 , "lif-ap" : 0.05467584931916471 , "ql-ap" : 0.251723934368579 }

fun LocalRetrieval.exec(expr: GExpr): QueryResults = QueryResults(this.transformAndExecuteQuery(expr).scoredDocuments)

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val rerankExperiment = argp.get("rerankExperiment", false)

    val operators = Parameters.create()
    operators.set("lib", LeastInformationBinary::class.java.canonicalName)
    operators.set("lif", LeastInformationFrequency::class.java.canonicalName)
    operators.set("libf", LIBtimesLIF::class.java.canonicalName)

    val indexP = Parameters.create()
    indexP.put("operators", operators)

    DataPaths.getRobustIndex(indexP).use { ret ->
        val tokenizer = ret.tokenizer!!

        val tqs = DataPaths.getTitleQueries()
        val qrels = DataPaths.getQueryJudgments()

        val evals = listOf(
                "ap"
                //, "ndcg10"
        ).associate { Pair(it, QueryEvaluatorFactory.create(it, Parameters.create())!!) }
        val measures = NamedMeasures()

        tqs.forEach { qid, qtext ->
            println(qid)
            val qterms = tokenizer.tokenize(qtext).terms.filterNotNull()
            val truth = qrels.get(qid)!!

            val libExpr = GExpr("wsum")
            val lifExpr = GExpr("wsum")
            val libfExpr = GExpr("wsum")
            val liflibSumExpr = GExpr("wsum")
            val qlExpr = GExpr("combine")
            qterms.forEach {
                qlExpr.push(GExpr.Text(it))
                libExpr.push(GExpr("lib").push(GExpr.Text(it)))
                lifExpr.push(GExpr("lif").push(GExpr.Text(it)))
                libfExpr.push(GExpr("libf").push(GExpr.Text(it)))

                // LIF+LIB
                liflibSumExpr.push(GExpr("lib").push(GExpr.Text(it)))
                liflibSumExpr.push(GExpr("lif").push(GExpr.Text(it)))
            }


            val tasks = mapOf(Pair("ql", qlExpr.clone()),
                    Pair("lib", libExpr),
                    Pair("lif", lifExpr), // this one's crap.
                    Pair("libf", libfExpr),
                    Pair("lif+lib", liflibSumExpr))

            tasks.forEach { (mName, expr) ->
                val gres = ret.transformAndExecuteQuery(expr)
                val qres = QueryResults(gres.scoredDocuments)
                if (mName == "libf") {
                    println(qres.take(10).joinToString { String.format("%1.3f", it.score) })
                } else if (rerankExperiment && mName == "lib") {
                    val qp = Parameters.create()
                    qp.set("working", ArrayList(gres.resultSet()))
                    val ql_lib_res = QueryResults(ret.transformAndExecuteQuery(qlExpr.clone(), qp).scoredDocuments)

                    evals.forEach { eName, eval ->
                        val score = eval.evaluate(ql_lib_res, truth)
                        measures.push("lib_ql.$eName", score)
                    }
                }

                evals.forEach { eName, eval ->
                    val score = eval.evaluate(qres, truth)
                    measures.push("$mName.$eName", score)
                }
            }
            println(measures.means())
        }
        println(Parameters.wrap(measures.means()))

        //ret.index.getIndexPart()
        //println(index..toPrettyString())
    }

}