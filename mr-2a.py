#Jack Kenney
#12.6.16
from mrjob.job import MRJob
from mrjob.job import MRStep

#To be run with "temp.csv" or "temp2.csv"

class MRTemp(MRJob):
#The Mapper function extracts the year from the csv file line.
	def mapper(self,_,line):
		year = line.split(',')[6] 
#Yield the year and a 1 for counting purposes.
		yield(year,1)
	
#The FIRST Reducer Function only passes a year forward if the count > 100
	def reducer_filter(self,year,one):
		if(len(list(one))>100):
			yield(0,year)

#The SECOND Reducer Function combines all of the years with key 0 
#	that were passsed on by the first reduction into a list.
	def reducer_comb_yrs(self,none,years):
		yield(0,list(set(years)))	

#The stepper defines first a mapper, then map filter > 100, then the combinator of years.
	def steps(self):
		return [
			MRStep(mapper=self.mapper,
					 reducer=self.reducer_filter),
			MRStep(reducer=self.reducer_comb_yrs)
		]
#Run tag when run from terminal.
if __name__ == '__main__':
	MRTemp.run()
