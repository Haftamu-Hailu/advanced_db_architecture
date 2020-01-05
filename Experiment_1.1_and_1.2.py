import time
import mmap
import contextlib
import os
import random

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
 
#Read one character at a time
@timerfunc
def read_char(file_src, block_size = -1, j = -1):
	sum = 0
	with open(file_src, 'r', buffering=2, encoding='utf-8', errors='ignore') as f:
		if j == -1:
			for line in f:
				sum = sum + len(line)
		else:
			#compute random number p between 0 and length of file
			for i in range(j):
				f.seek(0, 2) #set file pointer at the end
				p = random.randrange(0, f.tell()) 
				f.seek(p)
				line = f.readline()
				sum = sum + len(line)
	print("Number of characters read: ", sum)


#Read one line at a time
@timerfunc
def read_line(file_src, block_size = -1, j=-1):
	sum = 0
	with open(file_src, 'r', buffering=1, encoding='utf-8', errors='ignore') as f:
		if j == -1:
			for line in f:
				#print(line)
				sum = sum + len(line)
		else:
			#compute random number p between 0 and length of file
			for i in range(j):
				f.seek(0, 2) #set file pointer at the end
				p = random.randrange(0, f.tell()) 
				f.seek(p)
				line = f.readline()
				sum = sum + len(line)
	print("Number of characters read: ", sum)

#Read B buffeytes(and store them in buffer) at a time
@timerfunc
def read_bytes(file_src, block_size, j=-1):
	sum = 0
	with open(file_src, 'r', buffering=block_size, encoding='utf-8', errors='ignore') as f:
		if j == -1:
			for line in f:
				sum = sum + len(line)
		else:
			#compute random number p between 0 and length of file
			for i in range(j):
				f.seek(0, 2) #set file pointer at the end
				p = random.randrange(0, f.tell()) 
				f.seek(p)
				line = f.readline()
				sum = sum + len(line)
	print("Number of characters read: ", sum)

#Use memory mapping to read a file
@timerfunc	
def read_mmap(file_src, num_pages=0, j=-1):
	"""Block size should be multiple of page size"""
	sum = 0
	mapping = num_pages*mmap.ALLOCATIONGRANULARITY
	with open(file_src, 'r+b') as f:
		f.seek(0,2)
		filesize = f.tell()
		f.seek(0,0)
		prev_line = b''
		if j == -1:
			offset = 0			
			while mapping+offset < filesize:
				if mapping == 0:
					break			
				with mmap.mmap(f.fileno(), mapping, offset = offset, access=mmap.ACCESS_READ) as m:
					line = m.readline()
					line = prev_line+line					
					while line != b'':
						sum = sum + len(line.decode('utf-8'))
						#print(line.decode('utf-8'))
						line = m.readline()
						if len(line) != 0 and line[-1] != 10:
							prev_line = line
							break
					offset = offset + m.tell()

			#map the remaining part
			with mmap.mmap(f.fileno(), 0, offset=offset, access=mmap.ACCESS_READ) as m:
				line = m.readline()
				line = prev_line+line
				while line != b'':
					line=m.readline()
					sum = sum + len(line.decode('utf-8'))
		else:
			if mapping != 0:
				for i in range(j):
					p = random.randrange(0,filesize)
					n = int(p/mapping) 
					prev_line = b''
					if (n+1)*mapping > filesize: mem = 0 
					else: mem=mapping
					with mmap.mmap(f.fileno(), mem, offset = n*mapping, access=mmap.ACCESS_READ) as m:
						m.seek(p-n*mapping)
						line = m.readline()
						sum = sum + len(line)
			else:
				with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
					for i in range(j):
						p = random.randrange(0,filesize)
						m.seek(p)
						line = m.readline()
						sum = sum + len(line)
						
	print("Number of characters read: ", sum)

def helper_func(files, func, block_size, j):
	total_time = 0
	for file in files:
		total_time = total_time + func(file, block_size, j)
	return total_time


directory = './imdb/'
#print("mmap: ")
#read_mmap('mmap_mmap.txt', num_pages = 0, j=-1)
#print("line: ")
#read_char('mmap_mmap.txt', j=-1)



files = os.listdir(directory)
for i in range(len(files)):
	files[i] = directory+files[i]
files.sort(key = lambda f: os.stat(f).st_size)
filesizes = [os.stat(f).st_size/(10**9) for f in files]
#print(filesizes)
"""
#Experiment 1.1:
t1 = helper_func(files, read_char, -1, -1)
t2 = helper_func(files, read_line, -1, -1)
t3 = helper_func(files, read_bytes, 10, -1)
t4 = helper_func(files, read_bytes, 1000, -1)
t5 = helper_func(files, read_bytes, 100000, -1)
t6 = helper_func(files, read_mmap, 10, -1)
t7 = helper_func(files, read_mmap, 100, -1)
t8 = helper_func(files, read_mmap, 1000, -1)
t9 = helper_func(files, read_mmap, 0, -1)
print("{t1:.2}, {t2:.2}, {t3:.2}, {t4:.2}, {t5:.2}, {t6:.2}, {t7:.2}, {t8:.2}, {t9:.2}\n".format(t1 = t1, t2 = t2,\
				t3 = t3, t4=t4, t5=t5, t6=t6, t7=t7, t8=t8, t9=t9))
"""
"""
#Experiment 1.2:
t1 = helper_func(files, read_char, -1, 10000)
t2 = helper_func(files, read_line, -1, 10000)
t3 = helper_func(files, read_bytes, 10, 10000)
t4 = helper_func(files, read_bytes, 1000, 10000)
t5 = helper_func(files, read_bytes, 100000, 10000)
t6 = helper_func(files, read_mmap, 10, 10000)
t7 = helper_func(files, read_mmap, 100, 10000)
t8 = helper_func(files, read_mmap, 1000, 10000)
t9 = helper_func(files, read_mmap, 0, 10000)
print("{t1:.2}, {t2:.2}, {t3:.2}, {t4:.2}, {t5:.2}, {t6:.2}, {t7:.2}, {t8:.2}, {t9:.2}\n".format(t1 = t1, t2 = t2,\
				t3 = t3, t4=t4, t5=t5, t6=t6, t7=t7, t8=t8, t9=t9))

"""
t_mmap = []
t_line = []
for file in files:
	t_mmap.append(read_mmap(file, 0,10000))
	t_line.append(read_line(file, -1, 10000))
from matplotlib import pyplot as plt

plt.plot(filesizes, t_mmap)
plt.title("Execution time vs filesize using mmap for 10000 random reads")
plt.show()
plt.plot(filesizes, t_line)
plt.title("Execution time vs filesize using readline for 10000 random reads")
plt.show()
