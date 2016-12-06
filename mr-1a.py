#Jack Kenney
#12.6.16
from mrjob.job import MRJob

#To be run with "urldoc.txt"

class MRWordURLs(MRJob):
#The Mapper function splits the csv file into (url,doc) tuples based on commas.
		def mapper(self,_,line):
			(url,doc) = line.split(',')
#For each word in document (split by space) yield a key-value pair of the word and the url.
			for word in doc.split():
				yield word,url

#The Reducer function casts the list of URLs to a set and then back to remove duplicates.
		def reducer(self,word,urls):
			yield word, list(set(urls))

if __name__ == '__main__':
	MRWordURLs.run()

