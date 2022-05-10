/*--------------Group Members' Details-------------------*/
/* Name: Abhijay Mitra			Roll No: 18IE10001
   Name: Arghyadeep Bandyopadhyay	Roll No: 18EE10012
   Name: Debapriyo Mukherjee		Roll No: 18IE10035
   Name: Hrituraj Dutta			Roll No: 18EE35021
   Name: Shirsha Chowdhury		Roll No: 18EE10068
   Name: Subham Karmakar		Roll No: 18EE10067
*/


/*--------------Code for Assignment 2-----------------------*/

val path = "/home/arghyadeep/Wiki-Vote.txt"    //Path to input file. Path name to be changed accordingly
val data = sc.textFile(path)                   //RDD to store contents of input file

val data_graph = data.filter(line => {val s = line.split("\\s+"); s(0) != "#"}).map(line => line.split("\\s+"))   //Stores the edge data of the graph (lines not beginning with '#' symbol)
val links_auth = data_graph.map(node => (node(0).toInt, node(1).toInt)).distinct().groupByKey().cache() //For each node, stores the list of IDs of nodes having an incoming edge from this node
val links_hub = data_graph.map(node => (node(1).toInt, node(0).toInt)).distinct().groupByKey().cache()  //For each node, stores the list of IDs of nodes having an outgoing edge to this node

val node_list = (data_graph.map(lines => lines(0).toInt) ++ data_graph.map(lines => lines(1).toInt)).distinct()  //Stores the list of Node IDs
val n = node_list.count()     //Number of nodes

var auth_scores = node_list.map(node => (node, 1.00/n))  
var hub_scores = node_list.map(node => (node, 1.00/n))      //Authority and Hub scores initialised to 1/n

val iters = 100       //Number of iterations

for (i <- 1 to iters) 
{
   //For each node, the hub score contributed by a node having an in-link to this node is stored
   val contribs_hub = links_auth.join(hub_scores).values.flatMap{ 
                       case(nodes, hub_score) => { nodes.map(node => (node, hub_score))}}

   //For each node, the authority score contributed by a node having an out-link from this node is stored
   val contribs_auth = links_hub.join(auth_scores).values.flatMap{ 
                      case(nodes, auth_score) => { nodes.map(node => (node, auth_score))}}
   
   //Authority score for each node obtained by summation of hub scores of nodes having an in-link to that node
   auth_scores = contribs_hub.reduceByKey(_ + _)

   //Hub score for each node obtained by summation of authority scores of nodes having an out-link from that node
   hub_scores = contribs_auth.reduceByKey(_ + _)
   
   val sum_auth = auth_scores.values.sum()       //Sum of authority scores in current iteration 
   val sum_hub = hub_scores.values.sum()         //Sum of hub scores in current iteration

   auth_scores = auth_scores.map(pair => (pair._1, (pair._2)/sum_auth))     //Authority scores normalised  
   hub_scores = hub_scores.map(pair => (pair._1, (pair._2)/sum_hub))        //Hub scores normalised     
}

//As all nodes do not have a hub score and authority score assigned to it (due to the hub/authority score being 0), we need to set the hub/authority score to 0 for node keys absent in 'hub_scores' or 'auth_scores'
 
val rdd_init = node_list.map(node => (node, 0.00))   //RDD which stores key-value pairs with zero as value for each node

//A new RDD is obtained where for each node, if it is present in 'hub_scores' the hub score is stored, else zero is stored
val rdd_with_hub = (rdd_init ++ hub_scores).groupByKey.map(pair => { val node = pair._1;
                   val x = pair._2; var s = 0.00; if(x.size > 1) {if(x.head != 0.00) s = x.head; else s = x.last;}; (node,s)})

//A new RDD is obtained where for each node, if it is present in 'auth_scores' the authority score is stored, else zero is stored
val rdd_with_auth = (rdd_init ++ auth_scores).groupByKey.map(pair => { val node = pair._1;
                   val x = pair._2; var s = 0.00; if(x.size > 1) {if(x.head != 0.00) s = x.head; else s = x.last;}; (node,s)})

//For each node, the hub and authority scores are stored, in that order. The node keys are sorted in ascending order.
val rdd_allscores = rdd_with_hub.join(rdd_with_auth).sortByKey().map(pair => {pair._1 + " " + pair._2._1 + " " + pair._2._2})

val output_path = "/home/arghyadeep/hub_auth_scores"    //Path to store the output. Path name to be changed accordingly. 
val rdd_header = sc.parallelize(Seq("Node" + "\t" + "Hub Score" + "\t" + "Authority Score"))  //Names of columns (for better readibility)
val rdd_out = rdd_header ++ rdd_allscores
rdd_out.repartition(1).saveAsTextFile(output_path)
