import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Construction {
val sc = Initialisation.Spark.sc
var rddRes:RDD[String] = sc.emptyRDD

  def constructionArbre(sc:SparkContext,noeud: String, rdd: RDD[(String, String)]): RDD[String] = {

    val categorieDeClasse = rdd
      .map(x => (x._1.split(":")(0), x._1.split(":")(1).split("_")(1)))
      .reduceByKey(_+","+_)
      .zipWithIndex()
      .filter(x => (x._2 == 0))
      .map(x=> x._1)
      .map(x=> x._2)
      .flatMap(x=> x.split(","))
      .map(x=> (x,1))
      .reduceByKey(_+_)


    if (categorieDeClasse.count() > 1) {



      val total =  categorieDeClasse.map(x=> x._2).count().toInt


      val giniClasse = 1- categorieDeClasse
        .map(x=> carre(x._2.toDouble / total.toDouble )).sum()
      val kaka = rdd.count().toInt
      categorieDeClasse.unpersist()

      val kakaDD = rdd
        .map { case (x, y) => (x.split("_")(0), y.split(",").size+"") }
        .reduceByKey(_ + "#" + _)
        .map(x => if(!x._2.contains("#")){
          (x._1, x._2+"#0")
        }else (x._1,x._2)
        )


      val giniTest = kakaDD
        .map(x=> {
          (x._1,
            (carre(x._2.split("#")(0).toDouble /
              (x._2.split("#")(0).toDouble +x._2.split("#")(1).toDouble)))
              +
              carre(x._2.split("#")(1).toDouble /
                (x._2.split("#")(0).toDouble +x._2.split("#")(1).toDouble)))
        })
        .map(x=> (x._1, (1-x._2)))

      val testDeDivision = giniTest
        .reduce((x,y) => if (x._2< y._2) x else y)._1




      val num = rdd
        .map{case(x,y) =>(x.split("_")(0), y)}
        .filter{case(x,y) => (x.equals(testDeDivision))}
        .reduceByKey(_+","+_)
        .map{case(x,y) => y }.flatMap(x=> x.split(","))



      val ensemble = rdd.map(x=> (x._2,x._1))
      rdd.unpersist()

      val broadcastNumLignez = sc.broadcast(num.collect())
      num.unpersist()

      val sousEnsembles = rdd.map{case(x,y) => sep(x,y.split(","))}
        .flatMap(x=> x.split(","))
        .map(x=> (x.split("#")(1),x.split("#")(0)))
        .map{case (x,y) => (listContien(x,broadcastNumLignez), x+"#"+y)}

      for(etat<-0 to 1 ){
        var test2 =testDeDivision
        if(etat==0){
          test2=testDeDivision +"_non"
        }
        val unSousEnsemble = sousEnsembles.filter{case(x,y) => x == etat}
          .map{case(x,y) => (y.split("#")(1),y.split("#")(0))}
          .reduceByKey(_+","+_)
        var n=""
        if(!noeud.equals("")){
          n=noeud+"="
        }
        constructionArbre(sc,n+test2, unSousEnsemble.repartition(10))
      }


    }else{

rddRes = rddRes ++ sc.parallelize(Seq(noeud + "=" + categorieDeClasse.first()._1))
      println("taille "+rddRes.count())

    }

return rddRes

  }
  def carre(num:Double):Double={
    return Math.pow(num,2)
  }
  def sep(id:String,array: Array[String]): String ={
    var resultat=""

    for(i<-0 to array.size-1){
      if(resultat.equals("")){
        resultat = id+"#"+array(i)
      }else{
        resultat = resultat+","+id+"#"+array(i)
      }
    }

    return resultat
  }
  def listContien(x: String, broadcastNumLigne: Broadcast[Array[String]]) : Int = {
    var test=0
    // println("list contient : ")

    // println(" "+x+" "+broadcastNumLigne.value.toList)
    if(broadcastNumLigne.value.contains(x)) test = 1

    //  print("test "+test)
    //  scala.io.StdIn.readLine()
    return test
  }
  }
