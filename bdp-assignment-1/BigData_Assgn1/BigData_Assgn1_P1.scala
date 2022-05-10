/*--------------Group Members' Details-------------------*/
/* Name: Abhijay Mitra			Roll No: 18IE10001
   Name: Arghyadeep Bandyopadhyay	Roll No: 18EE10012
   Name: Debapriyo Mukherjee		Roll No: 18IE10035
   Name: Hrituraj Dutta			Roll No: 18EE35021
   Name: Shirsha Chowdhury		Roll No: 18EE10068
   Name: Subham Karmakar		Roll No: 18EE10067
*/
 

/*--------------Code for Problem 1-----------------------*/ 

val path = "/home/arghyadeep/SMSSpamCollection"  //Path to input file. Path name to be changed accordingly
val data = sc.textFile(path)                     //RDD to store the contents of input file 

//The messages in 'data' RDD are filtered into two RDDs on the basis of whether a message is 'spam' or 'ham'(non-spam) 
val data_spam = data.filter(line => {val s = line.split("\\W+"); s(0) == "spam"})     
val data_nonspam = data.filter(line => {val s = line.split("\\W+"); s(0) == "ham"})

//The list of stopwords is assumed to be stored in a text file "stopwords.txt". The contents of that file are now stored in a RDD
val stopWordsInput = sc.textFile("/home/arghyadeep/stopwords.txt")   

// Flatten, collect, and broadcast the stopwords
val stopWords = stopWordsInput.flatMap(x => x.split("\\s+")).map(_.trim)
val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)

//A map is created in the form of (tag, message) for both spam and non-spam messages. Tag is either 'spam' or 'ham' and message is the corresponding message in the form of an array of strings. The words in the message are converted to lowercase and then stopwords are removed. 

val rdd_spam = data_spam.map(line => {val s = line.split("\\W+")
                val len = s.length
		var w = new Array[String](len-1)
		for(i<-1 until len) {
			w(i-1) = s(i).toLowerCase()
		}
                val w_filtered = w.filter(!broadcastStopWords.value.contains(_)) 
		(s(0), w_filtered)})

val rdd_nonspam = data_nonspam.map(line => {val s = line.split("\\W+")
		val len = s.length
		var w = new Array[String](len-1)
		for(i<-1 until len) {
			w(i-1) = s(i).toLowerCase()
		}
                val w_filtered = w.filter(!broadcastStopWords.value.contains(_)) 
		(s(0), w_filtered)})

//All unique possible combinations of two words (w1,w2) in each message are obtained. Then a map is formed in the form of ((w1,w2),1)
//w1 and w2 are lexicographically ordered in the key (w1,w2). This is performed for both spam and non-spam messages.

val spam_comb = rdd_spam.flatMap(pair => pair._2.toList.distinct.combinations(2))
val spam_map = spam_comb.map(pair => ((if(pair(0) <= pair(1)) pair(0) else pair(1),if(pair(0) > pair(1)) pair(0) else pair(1)),1)) 

val nonspam_comb = rdd_nonspam.flatMap(pair => pair._2.toList.distinct.combinations(2))                                          
val nonspam_map = nonspam_comb.map(pair => ((if(pair(0) <= pair(1)) pair(0) else pair(1),if(pair(0) > pair(1)) pair(0) else pair(1)),1)) 

//Reduce operation to get count of SMSes containing (w1,w2) for all pairs in spam and non-spam messages
val sms_cnt_spam = spam_map.reduceByKey(_ + _)
val sms_cnt_nonspam = nonspam_map.reduceByKey(_ + _)

//The co-occurrence statistics for spam and non-spam SMSes are written in two separate files. 
//The format is <w1 w2 count>. The keys are sorted in ascending order.
val spam_output_path = "spam_word_pairs"
val spam_output = sms_cnt_spam.sortByKey().map(pair => pair._1._1 + " " + pair._1._2 + " " + pair._2)
spam_output.repartition(1).saveAsTextFile(spam_output_path)

val nonspam_output_path = "nonspam_word_pairs"
val nonspam_output = sms_cnt_nonspam.sortByKey().map(pair => pair._1._1 + " " + pair._1._2 + " " + pair._2)
nonspam_output.repartition(1).saveAsTextFile(nonspam_output_path)
