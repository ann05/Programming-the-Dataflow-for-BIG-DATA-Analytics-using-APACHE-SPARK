import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.io.IOException
import scala.util

object Trigrams2{
	def main(args: Array[String]){
		val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
		val sc = new SparkContext(conf)
		var hmap = scala.collection.mutable.Map[String,String] ()
		val br = new BufferedReader(new FileReader("new_lemmatizer.csv"))
		var line=""
		while ({  line = br.readLine();  line!= null}) {
			val line2=line.split(",")
			hmap += (line2(0) -> line2(1))
			}
		var i=0
		var j=0
		var k=1
		val temp=sc.textFile("Latindata")
		println("Anmisha")
		println(temp)	
		val temp2=temp.map(x => x.replace('j','i').replace('v','u'))
		var final_map=""
		val data=temp2.flatMap(_.split("\\r?\\n"))
		println(temp2)
		println(data)		
		val data_fin=data.flatMap(line => try{
					val test = line.split(">")
					var pos =test(0)
					pos += ">"
					//println(test(0))
					//println(test)
					//Int len= test.length _
					//if(len == 2){
					val data2=test(1).split(" ")
					//println(data2)
					var size = data2.length
					var data3 = new ListBuffer[String]()
					for{
						j <-0 until data2.length-1
						k <- (j+1) until data2.length-1
						i <- (k+1) until data2.length-1
							}{
							var t = "("
							if(hmap.contains(data2(j))){	t+=hmap.apply(data2(j))	}
							else{
								t+=data2(j)}
							t+=" , "
							if(hmap.contains(data2(k))){	t+=hmap.apply(data2(k))	}
							else{
								t+=data2(k)}
							t+=" , "
							if(hmap.contains(data2(i))){	t+=hmap.apply(data2(i))	}
							else{
								t+=data2(i)}
							t+=")"
							data3 += t
						
						}
					
					//println(data3)
					var data4=data3.toList
					//println("data3 "+data4)
					data4.map(x => (x,pos))
					//println(data4)
					//}
					}	catch{case e: Exception => None})
		
		val final_op = data_fin.reduceByKey((a, b) => a + b)
		//println(final_op.collect())
		//val out = new PrintWriter("final_map.txt")
		//val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output")))
		//val out2 = new PrintWriter("output.txt")
		//out.write(data_fin)
		//writer.write(final_op.collect())
		final_op.saveAsTextFile("output_tri_ex")
	}
}
