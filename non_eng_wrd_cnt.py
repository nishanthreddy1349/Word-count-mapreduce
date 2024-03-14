from pyspark import SparkContext
import re
from spellchecker import SpellChecker

# DOB : 13 April 2000

sc = SparkContext("local", "NonEnglishWordCount")

# here the file is loading from the path /users/nishanthreddy/desktop/assignment2/file2
file2 = sc.textFile("file2.txt")

# Using the spellcheck lirary we are trying to extract non english words.
spell = SpellChecker()

# here we are trying to remove non-alphabetic chars, extra space, converting text to lower case.
cleaned_data = file2.map(lambda line: re.sub(r'[^a-zA-Z]', ' ', line.lower()).strip())

# Filter out English words and count occurrences
non_english_words = cleaned_data.flatMap(lambda line: line.split()) \
                                 .filter(lambda word: not spell.known([word])) \
                                 .map(lambda word: (word, 1)) \
                                 .reduceByKey(lambda a, b: a + b)

# Print the results
for word, count in non_english_words.collect():
    print(f"{word}: {count}")


# Save the results to a text file
non_english_words.coalesce(1).saveAsTextFile("non_english_word_counts")
