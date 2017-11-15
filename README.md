# spark.FriendRecommendation
Implement friend recommendation based on number of mutual friends in Spark

### Map:
People in the friend list of a person, they are having one mutual friend. So, we emit a count ((a,b), 1) for a and a count ((b, a), 1) for b for the purpose of #mutual friends counting. <br />
For those who are friend already, we emit a negative count ((person id, his friend), -1).

### Reduce:
We group all those counts by tuple pair as key, and filter out those with a -1 value (already a friend), and reduce the count to a sum for each unique tuple pair. <br />
Finally, we can reshape, sort and extract the output as required.
