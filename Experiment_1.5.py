import time
import mmap
import contextlib
import os
from contextlib import ExitStack
from queue import Queue
from functools import reduce
import sys
import heapq
#Timeit decorator
def timerfunc(func):
    """
    A timer decorator
    """
    def function_timer(*args, **kwargs):
        """
        A nested function for timing other functions
        """
        start = time.time()
        value = func(*args, **kwargs)
        end = time.time()
        runtime = end - start
        msg = "The runtime for {func} took {time:.2f} seconds to complete"
        print(msg.format(func=func.__name__,
                         time=runtime))
        return runtime
    return function_timer
 

def write_mmap(data, output_src, num_pages):
	"""writes data to an output file using mmap, num_pages is number of pages to be mapped to file"""

	#convert data to binary
	
	#create a file
	filesize = sys.getsizeof(data)
	with open(output_src, 'w') as f:
		f.truncate(filesize)

	mapping = num_pages*mmap.ALLOCATIONGRANULARITY
	
	#write data to the file
	with open(output_src, 'r+') as f:
		offset = 0
		while data != b'' and mapping+offset < filesize:
			with mmap.mmap(f.fileno(), mapping, offset = offset, access=mmap.ACCESS_WRITE) as m:
				if mapping != 0:
					m.write(data[0:mapping])
					data = data[(mapping+1):]
					offset = offset + m.tell()
				else: #map the whole file at once to the memory
					m.write(data)
					data = b''				
		#map the remaining data
		if len(data) != 0:
			with mmap.mmap(f.fileno(), 0, offset=offset, access=ACCESS_WRITE) as m:
				m.write(data)

def extsort_helper(csv_line, k):
	'''returns the kth element of the csv_line'''
	key = csv_line.split(',')[k]
	return key
@timerfunc
def extsort(file_src, k, M, d):
	"""args:
	file_urc: path of the file to be read
	k: sort the file using kth column
	M: size of memory buffer, M can only be multiple of pagesize
	d: merging d items in the queue"""
	print("Input file size: ", os.stat(file_src).st_size)
	with open(file_src, 'r') as f:
		f.seek(0,2)
		filesize = f.tell()
		f.seek(0,0)
		prev_line = b''
		offset = 0
		n = 0
		while offset+M < filesize:			
			with mmap.mmap(f.fileno(), M, offset = offset, access=mmap.ACCESS_READ) as m:
				lines = []
				line = m.readline()
				line = prev_line+line
				while line != b'':
					lines.append(line.decode('utf-8'))
					line = m.readline()
					if len(line) != 0 and line[-1] != 10:
						prev_line = line
						break
				#sort the lines in the buffer
				lines.sort(key=lambda row: extsort_helper(row,k))
				#convert lines back to binary
				lines = [l.encode('utf-8') for l in lines]
				#write to the output file
				lines = reduce(lambda x, y: x+y, lines) #concatenate the list of lines
				#print(lines)
				#print(len(lines))
				write_mmap(lines, './extsort/partition_'+str(n)+".txt", 0)
				n = n+1

				offset = offset + m.tell()

		#map the remaining part
		with mmap.mmap(f.fileno(), 0, offset=offset, access=mmap.ACCESS_READ) as m:
			lines=[]
			line = m.readline()
			while line != b'':
				lines.append(line.decode('utf-8'))
				line=m.readline()

			#sort the lines in the buffer
			lines.sort(key=lambda row: extsort_helper(row,k))
			lines = [l.encode('utf-8') for l in lines]
			lines = reduce(lambda x, y: x+y, lines)
			write_mmap(lines, './extsort/partition_'+str(n)+".txt", 0)
			n = n+1

		#Now we have N/M sorted files on the disk
		#Load the file names into a queue
		q = Queue()
		for i in range(n):
			q.put('./extsort/partition_'+str(i)+'.txt')
		count = 0
		while q.qsize() > 1:
			#dequeue d files from the queue
			fs = []
			i=0			
			while not(q.empty()) and i < d: 
				fs.append(open(q.get(), 'r', buffering = 1))
				i = i+1

			print(q.qsize(), end=", ")
			
			#Build a heap for storing lines
			h = [] 
			for j in range(len(fs)):
				line = fs[j].readline()
				heapq.heappush(h, (line.split(',')[k], j, line))

			with open('./extsort/merge_'+str(count)+".txt", 'w', buffering=1) as outfile:
				while len(h) != 0:
					#pop the smallest element from the heap
					element = heapq.heappop(h)
					#print(element)
					j, line = element[1], element[2]
					outfile.write(line)
					line = fs[j].readline()
					#print(len(h))
					if line != "":
						try:
							heapq.heappush(h, (line.split(',')[k], j, line))
						except:
							print(line)
			#close the files 
			for file in fs:
				file.close()
			#add the merged file to the queue
			q.put('./extsort/merge_'+str(count)+".txt")
			count = count+1
		#output final sorted file
		sorted_file = q.get()
		print("Final sorted file: ", sorted_file, " with size ", os.stat(sorted_file).st_size)

if __name__ == "__main__":
	
	#Experiment with different M and d
	d = 10
	k = 0
	filename = "./imdb/aka_name.csv"
	sorted_filename = "merge_1.txt"
	PAGESIZE = mmap.ALLOCATIONGRANULARITY #4096
	M = 1*PAGESIZE
	extsort(filename, k, M, d)
	extsort(sorted_file, k, M, d)
	M = 10*PAGESIZE
	extsort(filename, k, M, d)
	extsort(sorted_file, k, M, d)
	M = 100*PAGESIZE
	extsort(filename, k, M, d)
	extsort(sorted_file, k, M, d)
	M = 1000*PAGESIZE
	extsort(filename, k, M, d)
	extsort(sorted_file, k, M, d)

	#Select M = 10*PAGESIZE as optimum
	M = 10*PAGESIZE
	d = 10
	extsort(filename, k, M, d)
	extsort(sorted_file, k, M, d)
	d = 50 
	extsort(filename, k, M, d)
	extsort(sorted_file, k, M, d)
	d = 100
	extsort(filename, k, M, d)
	extsort(sorted_file, k, M, d)

	#Time vs the size of the file
	directory = "./imdb/"
	files = os.listdir(directory)
	for i in range(len(files)):
		files[i] = directory+files[i]
	files.sort(key = lambda f: os.stat(f).st_size)
	files = files[:10]
	filesizes = [os.stat(f).st_size/(10**6) for f in files]

	t_msort = []
	for file in files:
		t_msort.append(extsort(file, k, M, d))
	
	from matplotlib import pyplot as plt

	plt.plot(filesizes, t_mmap)
	plt.title("Execution time vs filesize for multi-way merge sort")
	plt.show()"""

