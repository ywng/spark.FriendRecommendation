/* PeopleYouMayKnow.scala */
package assignment1

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PeopleYouMayKnow {

  private var numOfRecommendations = 10;

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("People You May Know")
    val sc = new SparkContext(conf)

    if(args.length < 2) {
      println("Usage: spark-submit --class <className> --master <masterNode> <jar location> <input location> <output location>")
      sc.stop()
    }

    val friendConnectionFile = args(0)
    val friendConnectionData = sc.textFile(friendConnectionFile)

    /* We don't need to process those person whose friend list is empty.
     * It is equivalent that no one will include them in their friend list, so no friend recommendation of them will be generted.
     * They will not have friend being recommended as well, as no one has mutual friend with them.
     * So, just append them at the end of result file with no friend recommendation.
     */
    val personWithNoFriend = friendConnectionData.map(line => line.split("\t")).filter(arr => arr.size <= 1)
    val noFriendOutput = personWithNoFriend.map(arr => arr(0) + "\t" )

    // (person id, friend list)
    val splitedfriendConnection = friendConnectionData.map(line => line.split("\t")).filter(arr => arr.size > 1)
    /* List[(person id, his friend)]
     * This is to give entries who and who are already friend, assign a -1, 
     * so that the count can be filtered out in recommendation processing.
     */
    val friendLinks = splitedfriendConnection.flatMap( 
                            arr => arr(1).split(",").map(friend => {
                                ((arr(0).toInt, friend.toInt), -1)
                            })
                        ).cache()

    /* People in the friend list of a person, they are having one mutual friend. 
     * So, we emit a count ((a,b), 1) for a and a count ((b, a), 1) for b for the purpose of #mutual friends counting.
     */
    val mutualFriendLinks = splitedfriendConnection.map( arr => arr(1).split(",")).
                                flatMap( arr => arr.combinations(2)).
                                flatMap(mutualFriendComb => {
                                    val comb1 = ((mutualFriendComb(0).toInt, mutualFriendComb(1).toInt), 1)
                                    val comb2 = ((mutualFriendComb(1).toInt, mutualFriendComb(0).toInt), 1)
                                    List(comb1, comb2)  
                                 }).cache()

    val combined = friendLinks.union(mutualFriendLinks)
     
    //Group by key (person, person) and filter out the key having -1 because they are already list.
    //Then sum how many mutual friends they have.
    val result = combined.groupByKey().filter(x => x._2.count(_ == -1) ==0).mapValues(_.sum)

    //Now we change the key-values to format (person, (friendRecommended, commandFriendCount)), and sort it descending
    val resultSorted = result.map(link => (link._1._1, (link._1._2, link._2) )).groupByKey().
                        mapValues(_.toSeq.sortBy(recom => (-recom._2, recom._1)).take(PeopleYouMayKnow.numOfRecommendations)).cache()

    val resultOutput = resultSorted.map(entry => {
        val recommendList = (entry._2).map(x=> x._1).mkString(" ")
        val outputLine  = entry._1 + "\t" + recommendList
        outputLine 
    })
    //combine two output to include those who has no friend.
    resultOutput.union(noFriendOutput).repartition(1).saveAsTextFile(args(1) + "FriendRecommendation")
    
    val resultFiltered = resultSorted.filter(re => re._1 == 8941 || re._1 == 9020 || re._1 == 9021 || re._1 == 9993 )
    val resultOutputFiltered = resultFiltered.map(entry => {
        val recommendList = (entry._2).map(x=> x._1).mkString(" ")
        val outputLine  = entry._1 + "\t" + recommendList
        outputLine 
    })
    resultOutputFiltered.repartition(1).saveAsTextFile(args(1) + "FriendRecommendation_Filtered")

    sc.stop()
  }
}