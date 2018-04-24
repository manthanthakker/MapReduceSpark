package newdriver

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object randomforest {

	def getRandomForestClassifier(): RandomForestClassifier = {
			new RandomForestClassifier()
			.setLabelCol("_1")
			.setFeaturesCol("_2").setImpurity("gini").setMaxDepth(12)
			
	}
	

	def main(args: Array[String]): Unit = {
	case class Feature1(x: Seq[Double], label: Double)

	// Job boiler plate code
	Logger.getLogger("org").setLevel(Level.ERROR)
	val spark: SparkSession = SparkSession.builder.appName("RandomForest").getOrCreate()
	val sQLContext = spark.sqlContext

	// Prepare the test Data Frame


	import spark.implicits._

	val testDF = spark.sparkContext.textFile(args(0)).mapPartitions(iteratedstring 
			=> {
				iteratedstring
				.map(x=> x.split(","))
				.map(row => (row(3087).toDouble, Vectors.dense(row.slice(0, 3086).map(_.toDouble))))

			},true).toDF()
	
			
	/**
	 * split the dataframe randomnly 0.7 or 70 pc for training and 30 percent for testing(validation)		
	 */
	val splitDF=testDF.randomSplit(Array[Double](0.7,0.3), 1)
	
	
	/**
	 * training data 
	 */
	val trainingData=splitDF(0)
	
	/**
	 * testing data 
	 */
	val testingData=splitDF(1)
	

	/**
	 * count the total number of 1's in the testing data 
	 */
	val actualones = testingData.select("_1").rdd.filter(x => (x(0) == 1.0)).count()

	/**
	 * random forest classifier 
	 */
	val rf = getRandomForestClassifier()
	
	val outputString="";
	
	  // set number of trees for each iteration 
	  rf.setNumTrees(10)
	  
	  
	  /**
	   * train the model with the training data 
	   */
	  val model = rf.fit(trainingData)
	  
	  
	  /**
	   * write the model trained 
	   */
	  model.write.overwrite().save(args(2))

	// Test the model
	val results = model.transform(testingData)

	/**
	 * after transforming, get the labels (not predicted,already existing ) and the predicted columns 
	 */
	val correctOnes=results.select("_1","prediction").rdd

	/**
	 * if the col0 and col1 match for row0, make it 1, else 0 
	 */
	
	val accuracyOfData=correctOnes.mapPartitions(iteratedrow => {
	  iteratedrow.map(row => if(row(0)==row(1)) 1 
		else 0)
	  
	},true
	).sum()

	/**
	 * count the actual data present 	
	 */
	val count=testingData.count()

	
	/**
	 * final accuracy is accurate predictions divided by total data 
	 */
	val FinalAcc=accuracyOfData/count

	// Calculate the accuracy
	/**
	 * count the number of predicted ones in the rdd 
	 */
	
	val predicictionones = results.select("prediction").rdd.filter(x => (x(0) == 1.0)).count()

	/**
	 *get the accuracy by dividing by 100.0 
	 */
	val errorInOnes = Math.abs((predicictionones - actualones)) / (actualones*1.0)

	try {

	  /**
	   * print the output predictions and other values 
	   */
	  
	  
		val ArrayOfValues=Array("NumTrees "+10,"FinalAccuracy "+FinalAcc,"PredictedOnes "+predicictionones,"ActualOnes "+actualones,"AccuracyOfModel========= "+errorInOnes,"AccuracyofDataEquals"+accuracyOfData)
		/**
		 * save the output parameters in a text file 
		 */
		val r= spark.sparkContext.parallelize(ArrayOfValues, 1).saveAsTextFile(args(1))
	}
	catch {
	case e: Exception =>{ 
		Logger.getLogger("org").setLevel(Level.ERROR)
		println(e.printStackTrace())
	}

	
	}

	}

}
