/*--------------Group Members' Details-------------------*/
/* Name: Abhijay Mitra			Roll No: 18IE10001
   Name: Arghyadeep Bandyopadhyay	Roll No: 18EE10012
   Name: Debapriyo Mukherjee		Roll No: 18IE10035
   Name: Hrituraj Dutta			Roll No: 18EE35021
   Name: Shirsha Chowdhury		Roll No: 18EE10068
   Name: Subham Karmakar		Roll No: 18EE10067
*/
 

/*--------------Code for Problem 2-----------------------*/

import java.io._

//Loading the output of the previous code. Path names to be changed accordingly
val rdd_nonspam = sc.textFile("/home/arghyadeep/nonspam_word_pairs/part-00000")  
val rdd_spam = sc.textFile("/home/arghyadeep/spam_word_pairs/part-00000")  

//PrintWriter to write to output file. Path name to be changed accordingly     
val pw = new PrintWriter(new File("/home/arghyadeep/most_frequent_words.txt"))   
  
//create a map of (List(w1,w2),count(int)) for non spam and spam
val rdd_nonspam1 = rdd_nonspam.map(line => {val s = line.split("\\s+")  
		val mylist1: List[String] = List(s(0),s(1))
		(mylist1, s(2).toInt)})

val rdd_spam1 = rdd_spam.map(line => {val s = line.split("\\s+")  
		val mylist1: List[String] = List(s(0),s(1))
		(mylist1, s(2).toInt)})

//List of words is defined 
val wordList:List[String]=List("super","friend","colour")

//For each word take the top 5 occurences
pw.write("The words are 'super'(given) ,'friend','colour'; 'friend' and 'colour' have been chosen by us.")
pw.write("\n")

wordList.foreach(word=>{
	
	pw.write(word+":The top 5 coccurences")
	pw.write("\n")

	//Store the rdd when the word is the first occurence
	var rdd_nonspam_part1 = rdd_nonspam1.filter(a=>(a._1(0)==word))
	var rdd_spam_part1 = rdd_spam1.filter(a=>(a._1(0)==word))
	
        //Store the rdd when the word is the second occurence
	var rdd_nonspam_part2 = rdd_nonspam1.filter(a=>(a._1(1)==word))
	var rdd_spam_part2 = rdd_spam1.filter(a=>(a._1(1)==word))
	
        //Create an array of pair of (list,int) 	
	var arr_nonspam = rdd_nonspam_part1.collect() ++ rdd_nonspam_part2.collect()
	var arr_spam = rdd_spam_part1.collect() ++ rdd_spam_part2.collect()
	
        //Sort the arrays in descending order by count
	arr_nonspam = arr_nonspam.sortBy(x=>(x._2)).reverse
	arr_spam = arr_spam.sortBy(x=>(x._2)).reverse
	
        //Take the top 5 most frequently occurring words
        var finArr_nonspam = arr_nonspam.take(5)
	var finArr_spam = arr_spam.take(5)

        //Write to file for non-spam
	pw.write("For Non Spam:")
	pw.write("\n")
	finArr_nonspam.foreach(x=>{
      	if(x._1(0)==word){
     		pw.write(x._1(1))
      }
      else{
      	
		pw.write(x._1(0))
      }
	pw.write("\n")
      })

        //Write to file for spam
	pw.write("\n")
	pw.write("For Spam:")
	pw.write("\n")
	finArr_spam.foreach(x=>{
      	if(x._1(0)==word){
     		pw.write(x._1(1))
      }
      else{
      	
		pw.write(x._1(0))
      }
	pw.write("\n")
      })
	pw.write("\n")
}
)
pw.close
