import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path

object Accueil {

  var compteur = 0
  val sc = Initialisation.Spark.sc
  val cmp = sc.longAccumulator("compte")
  def main(args:Array[String]): Unit = {


    println("................................................... ")
    val debut = "CREATION ... [DEBUT] : " + Calendar.getInstance().getTime.getMinutes + ":" + Calendar.getInstance().getTime.getSeconds
    val fichierEntee =  "/home/yacinou/Desktop/dataSet/respiratory.csv"
    val dossierSortie = "/home/yacinou/Desktop/resultatCART"


    val rdd = sc.textFile(fichierEntee)


    val ensembleStructure = rdd.zipWithIndex()
      .map { case (x, y) =>
        structurerEnsemble(y.toInt, x.split(","))
      }
          .flatMap(x=> x.split(" "))
      .map(x => (x.split(":")(0),x.split(":")(1)))
      .reduceByKey(_+","+_)



    val ensembleRegroupe  = ensembleStructure
      .map{case(x,y) => (separerValeurs(" ",x,y))}
      .flatMap(x=> x.split(" "))
      .map(x=> (x.split("#")(0), x.split("#")(1)))
      .reduceByKey(_+","+_).cache()

    ensembleRegroupe.foreach(x=> println(x._1+" .. "+x._2))
val res = Construction.constructionArbre(sc,"",ensembleRegroupe)

    println(res.count())
    println("fin ")

   enregistreArbre(debut,sc,res,dossierSortie)



  }

  def enregistreArbre(debut:String,sc: SparkContext, resultat:RDD[String],output: String): Unit = {

    if (scala.reflect.io.File(scala.reflect.io.Path(output)).exists) {
      val jj: Path = Path(output)
      jj.deleteRecursively()
      resultat.coalesce(1, true).saveAsTextFile(output)
    } else {
      resultat.coalesce(1, true).saveAsTextFile(output)
    }
    println(debut)

    println("................................................... ")
    println(" [Fin] : " + Calendar.getInstance().getTime.getMinutes + ":" + Calendar.getInstance().getTime.getSeconds)

  }










  def structurerEnsemble(numLigne:Int,array: Array[String]): String = {
    var result =""
    for(i<-0 to array.size-2){
      if (result.equals("")){
        result =  i + ":" + array(i) + "_" + array(array.size - 1)+"#"+numLigne
      }else{
        result = result+" "+ i + ":" + array(i) + "_" + array(array.size - 1)+"#"+numLigne
      }
    }

    return result
  }

  def separerValeurs(SEPARATEUR:String,cle:String,texte:String):String={
    var resultat:String=""
    val array=texte.split(",")
    for(i<-0 to array.size-1){
      if(resultat.equals("")){
        resultat = cle+":"+array(i)
      }else{
        resultat = resultat+SEPARATEUR+cle+":"+array(i)
      }
    }
    return resultat
  }


}
