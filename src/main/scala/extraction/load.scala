package extraction
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

case class Temperature(year:Int,month:Int,day:Int,mrng:Float,noon:Float,eveng:Float)
case class Temperatureminmax(year:Int,month:Int,day:Int,mrng:Float,noon:Float,eveng:Float,tmin:Float,tmax:Float)
case class Temperatureminmaxmean(year:Int,month:Int,day:Int,mrng:Float,noon:Float,eveng:Float,tmin:Float,tmax:Float,mean:Float)

case class Pressure(year:Int,month:Int,day:Int,mrngpress:Float,noonpress:Float,evengpress:Float)
case class Pressure9(year:Int,month:Int,day:Int,mrngobs:Float,mrngtemp:Float,noonobs:Float,noontemp:Float,evengobs:Float,evengtemp:Float)
case class Pressure12(year:Int,month:Int,day:Int,mrngobs:Float,mrngtemp:Float,mrngpress:Float,noonobs:Float,noontemp:Float,noonpress:Float,evengobs:Float,evengtemp:Float,evengpress:Float)

object load {
	def main(args:Array[String])={
			    var homedir="/home/manish/ica/"
					val category=args(0)
					val bufferedsource=scala.io.Source.fromFile(category+".csv")
					var outputdir=homedir+"/output/"+category
					homedir=homedir+category
					val spark = SparkSession.builder().master("local").appName("ica").getOrCreate()
					for(line<-bufferedsource.getLines.drop(1)){
						val cols=line.split(",").map(_.trim)
								println(s"${cols(0)}|${cols(1)}")
								val key=cols(1).split("_")(1)+"_"+cols(1).split("_")(2)
								val content=scala.io.Source.fromURL(s"${cols(0)}")
								var path = homedir+"/"+cols(1)
								val out = new java.io.FileWriter(path)
								out.write(content.mkString)
								out.close
								if(category=="Temperature"){
									loadingTemp(spark,path,key,outputdir) 
								}
								else if(category=="Pressure"){
									loadingPressure(spark,path,key,outputdir) 
								}
					}
			//merging different schemas

			val mergedDF = spark.read.option("mergeSchema", "true").parquet(outputdir) 
					//writing the dataframe on hive tables
			mergedDF.write.partitionBy("key").mode("append").format("parquet").saveAsTable(category)
	}

	def loadingTemp(spark:SparkSession,path:String,key:String,outputdir:String):Unit={
			//Loading temperate files to spark dataframes and storing them as parquet files
			import spark.implicits._
			val myFile = spark.sparkContext.textFile(path)
			val len= myFile.take(1)(0).replaceAll("^\\s","").split("\\s+").size

			if(len==6){
				val filetoDF=myFile.map(_.replaceAll("^\\s","")).map(_.split("\\s+")).map{x=>Temperature(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toFloat,x(4).toFloat,x(5).toFloat)}.toDF()
						//Basic Integrity check
						if(myFile.count==filetoDF.count){
							filetoDF.write.parquet(outputdir+"/key="+key) 
						}
						else{
							print("file at:"+path+"is corrupt")
						}

			}
			else if(len==8){
				val filetoDF=myFile.map(_.replaceAll("^\\s","")).map(_.split("\\s+")).map{x=>Temperatureminmax(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toFloat,x(4).toFloat,x(5).toFloat,x(6).toFloat,x(7).toFloat)}.toDF() 
						//Basic Integrity check
						if(myFile.count==filetoDF.count){
							filetoDF.write.parquet(outputdir+"/key="+key) 
						}
						else{
							print("file at:"+path+"is corrupt")
						} 
			}
			else if(len==9){
				val filetoDF=myFile.map(_.replaceAll("^\\s","")).map(_.split("\\s+")).map{x=>Temperatureminmaxmean(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toFloat,x(4).toFloat,x(5).toFloat,x(6).toFloat,x(7).toFloat,x(8).toFloat)}.toDF()
						//Basic Integrity check
						if(myFile.count==filetoDF.count){
							filetoDF.write.parquet(outputdir+"/key="+key) 
						}
						else{
							print("file at:"+path+"is corrupt")
						}

			}

	}
	def loadingPressure(spark:SparkSession,path:String,key:String,outputdir:String):Unit={
			//loading air pressure files to dataframe and saving them as parquet files
			import spark.implicits._
			val myFile = spark.sparkContext.textFile(path)
			val len= myFile.take(1)(0).replaceAll("^\\s","").split("\\s+").size

			if(len==6){
				val filetoDF=myFile.map(_.replaceAll("^\\s","")).map(_.split("\\s+")).map{x=>Pressure(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toFloat,x(4).toFloat,x(5).toFloat)}.toDF()
						//Basic Integrity check
						if(myFile.count==filetoDF.count){
							filetoDF.write.parquet(outputdir+"/key="+key) 
						}
						else{
							print("file at:"+path+"is corrupt")
						}
			}
			else if(len==9){
				val filetoDF=myFile.map(_.replaceAll("^\\s","")).map(_.split("\\s+")).map{x=>Pressure9(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toFloat,x(4).toFloat,x(5).toFloat,x(6).toFloat,x(7).toFloat,x(8).toFloat)}.toDF() 
						//Basic Integrity check
						if(myFile.count==filetoDF.count){
							filetoDF.write.parquet(outputdir+"/key="+key) 
						}
						else{
							print("file at:"+path+"is corrupt")
						} 
			}
			else if(len==12){
				val filetoDF=myFile.map(_.replaceAll("^\\s","")).map(_.split("\\s+")).map{x=>Pressure12(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toFloat,x(4).toFloat,x(5).toFloat,x(6).toFloat,x(7).toFloat,x(8).toFloat,x(9).toFloat,x(10).toFloat,x(11).toFloat)}.toDF()
						//Basic Integrity check
						if(myFile.count==filetoDF.count){
							filetoDF.write.parquet(outputdir+"/key="+key) 
						}
						else{
							print("file at:"+path+"is corrupt")
						}
			}
			else{
				print("Rdd is corrupt")
			}

	}
}