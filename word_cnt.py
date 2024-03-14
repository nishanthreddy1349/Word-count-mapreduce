from pyspark import SparkContext
import re

# DOB : 13 April 2000

sc = SparkContext("local", "WordCount")

# here the file is loading from the path /users/nishanthreddy/desktop/assignment2/file1
file1 = sc.textFile("file1.txt") 

# here we are trying to remove non-alphabetic chars, extra space, converting text to lower case.
cleaned_data = file1.map(lambda line: re.sub(r'[^a-zA-Z]', ' ', line.lower()).strip()) 

# Counting occurances of each word
word_counts = cleaned_data.flatMap(lambda line: line.split()) \
                          .map(lambda word: (word, 1)) \
                          .reduceByKey(lambda a, b: a + b)

# Print the results
for word, count in word_counts.collect():
    print(f"{word}: {count}")


# Save the results to a text file
word_counts.coalesce(1).saveAsTextFile("word_counts")
