#Jack Kenney
#12.6.16
from mrjob.job import MRJob

#To be run with "temp2.csv"

class MRTemp(MRJob):
#The mapper function
	def mapper(self,_,line):
		cont = line.split(',')[3]
		year = line.split(',')[6]
		temp = line.split(',')[7]
#Yield a result of paired continents and years with the temp cast to integers.
		yield((cont,year),int(temp))

#The reducer function
	def reducer(self,tup,temps):
#For some reason, len(list(temps)) would return zero and sum wouldn't work
#Due to the state of generator objects throwing away their length values
#Thus, I proceeded the long way.
		l = 0
		s = 0
		for x	in list(temps):
			l = l + 1
			s = s + x
#If the length of the list is greater than 50, yield a result and compute the average.
		if(l > 50):
			avg = s/l
			yield(tup,avg)

if __name__ == '__main__':
	MRTemp.run()
