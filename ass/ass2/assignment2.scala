// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "/tmp_amd/reed/export/reed/2/z5182060/9313/sample_input"
val outputDirPath = "/tmp_amd/reed/export/reed/2/z5182060/9313/output"


// Write your solution here
// create RDD
val File = sc.textFile(inputFilePath,1)

//split each line and map the URL and playload in a Array
val SplitLinesPairs = File.map(line => (line.split(",")(0),line.split(",")(3)))

//def function change MB & KB to B
def PairMatch(a:String):Long = {
    if(a.substring(a.length()-2,a.length())=="MB") {
        val result:Long = (a.substring(0,a.length()-2)).toLong * 1024 * 1024
        return result
    } else if (a.substring(a.length()-2,a.length())=="KB") {
        val result:Long = (a.substring(0,a.length()-2)).toLong * 1024
        return result
    } else {
        return (a.substring(0,a.length()-1)).toLong
    }
}

//change MB & KB to B without unit
val BPairs = SplitLinesPairs.map(x =>(x._1,PairMatch(x._2)))

//def function caculate the min max mean and variance
def CaResult(a:Iterable[Long]):String = {
    val MinValue:Long = a.min
    val MaxValue:Long = a.max
    val SumValue:Long = a.sum
    val SizeValue:Long = a.size
    val MeanValue:Long = SumValue / SizeValue
    var PreVar:Long = 0
    a.foreach(x=> PreVar = PreVar + (x - MeanValue) * (x - MeanValue))
    val VarianceValue:Long = PreVar / SizeValue
    val Result:String = String.valueOf(MinValue)+"B,"+String.valueOf(MaxValue)+"B,"+String.valueOf(MeanValue)+"B,"+String.valueOf(VarianceValue)+"B"
    return Result
}

//group and caculate the result
val AftGroup = BPairs.groupByKey()
val FinalResult = (AftGroup.map(x => (x._1,CaResult(x._2)))).sortByKey()

//map the result to one string
val OutputResult = FinalResult.map(x => x._1+","+x._2)

// output the result
OutputResult.saveAsTextFile(outputDirPath)
