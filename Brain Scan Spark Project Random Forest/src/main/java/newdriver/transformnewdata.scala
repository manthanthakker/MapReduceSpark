package newdriver

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession


object transformnewdata {


	def main(args: Array[String]): Unit = {
	case class Feature1(x: Seq[Double], label: Double)

	
	Logger.getLogger("org").setLevel(Level.ERROR)
	val spark: SparkSession = SparkSession.builder.appName("RandomForestTrainData").getOrCreate()


	val sQLContext = spark.sqlContext

	// Prepare the test Data Frame


	import spark.implicits._

	/**
	 * load the dataframe that needs to be transformed  
	 */
	val loadedDF = spark.sparkContext.textFile(args(0)).mapPartitions(iteratedstring 
			=> {
				iteratedstring
				.map(x=> x.split(",")) // dummy 0.0 as label which wont affect the prediction 
				.map(row => (0.0, Vectors.dense(row.slice(0, 3086).map(_.toDouble))))

			},true).toDF()


	import org.apache.spark.ml._
	import org.apache.spark.ml.classification.RandomForestClassificationModel;

	/**
	 * load the RandomForestClassificationModel from filepath 
	 */
	val loadModel =RandomForestClassificationModel.load(args(1))

			val outputString="";

	// Test the model
	val results = loadModel.transform(loadedDF)

			/**
			 * get the transformed dataframe and make a rdd of its prediction  
			 */
			val finalpredictions=results.select("prediction").rdd

			/**
			 * if the row0==1.0, add 1 to rdd else 0 and finally take its sum 
			 */


			val countOfOnes=finalpredictions.mapPartitions(iteratedrow => {
				iteratedrow.map(row => if(row(0)==1.0) 1
						else 0)

			},true
					).sum()
					

					/**
					 * similarly for 0's 
					 */
					val countOfzeros=finalpredictions.mapPartitions(iteratedrow => {
				iteratedrow.map(row => if(row(0)==0.0) 1
						else 0)

			},true
					).sum()


			/**
			 * count the actual data present 	
			 */
			val count=loadedDF.count()

			/**
			 * get the percent of 0's and 1's found 
			 */
			val percentZeroes=countOfzeros/(count*1.0)
			val percentOnes=countOfOnes/(count*1.0)

			try {
				
			  /**
			   * print the parameters and the output file 
			   */
						val ArrayOfValues=Array("PredictedOnes "+countOfOnes,"PredictedZeroes "+countOfzeros,"percentZeroes "+percentZeroes,
								"percentOnes "+percentOnes,"countofdata"+count)

						val r= spark.sparkContext.parallelize(ArrayOfValues, 1).saveAsTextFile(args(2))
						
						val finalPredictionsArray=finalpredictions.collect()

						spark.sparkContext.parallelize(finalPredictionsArray, 1).saveAsTextFile(args(3))
						

			}
	catch {
	case e: Exception =>{ 
		Logger.getLogger("org").setLevel(Level.ERROR)
		println(e.printStackTrace())
	}


	}

	}


}