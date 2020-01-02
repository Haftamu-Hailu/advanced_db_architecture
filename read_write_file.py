import time
import mmap
import contextlib
import os
import random
from contextlib import ExitStack

#Timeit context manager
class MyTimer():
 
    def __init__(self):
        self.start = time.time()
 
    def __enter__(self):
        return self
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        runtime = end - self.start
        msg = 'The function took {time:.2f} seconds to complete'
        print(msg.format(time=runtime))

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
def read_one_char(file_url, j = -1):
	sum = 0
	with open(file_url, 'r', buffering=2, encoding='utf-8', errors='ignore') as f:
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

		

"""
def write_one_char(filename, literal):
	with open(file_url, 'a', buffering=0) as f:
		f.write(literal)
"""

#Read one line at a time
@timerfunc
def read_one_line(file_url, j=-1):
	sum = 0
	with open(file_url, 'r', buffering=1, encoding='utf-8', errors='ignore') as f:
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

"""
def write_one_line(filename, line):
	sum = 0
	with open(filename, 'a', buffering=1) as f:
		for line in f:
			sum = sum + len(line)
	return sum
"""

#Read B bytes(and store them in buffer) at a time
@timerfunc
def read_bytes(file_url, block_size, j=-1):
	sum = 0
	with open(file_url, 'r', buffering=block_size, encoding='utf-8', errors='ignore') as f:
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

"""
#Write B bytes
def write_bytes(file_url, block_size, literal):
	with open(file_url, 'a', buffering=block_size) as f:
		f.write(literal)
"""

#Use memory mapping to read a file
@timerfunc
def read_with_memory_mapping(file_url, j=-1):
	"""Block size should be multiple of page size"""
	sum = 0
	with open(file_url, 'r') as f:
		with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
			if j == -1:
				for line in m:
					sum = sum + len(line)
			else:
				for i in range(j):
					m.seek(0,2)
					p = random.randrange(0,m.tell())
					m.seek(p)
					line = f.readline()
					sum = sum + len(line)
	print("Number of characters: ", sum)

"""
def write_with_memory_mapping(file_url, block_size, literal):
	with open(file_url, 'a') as f:
		with contextlib.closing(mmap.mmap(f.fileno(), block_size, access=mmap.ACCESS_WRITE)) as m:
			m.write(literal)
"""

directory = './imdb/'
#read_one_line('./imdb/cast_info.csv')
"""
#Experiment 1.1:
with open('Experiment_1.1_results.txt', 'w') as f: 
	for file in os.listdir(directory):
		t1 = read_one_char(directory+file)
		t2 = read_one_line(directory+file)
		t3 = read_bytes(directory+file, 10)
		t4 = read_bytes(directory+file, 100)
		t5 = read_bytes(directory+file, 1000)
		t6 = read_bytes(directory+file, 10000)
		t7 = read_with_memory_mapping(directory+file)
		line = "{t1:.2}, {t2:.2}, {t3:.2}, {t4:.2}, {t5:.2}, {t6:.2}, {t7:.2}\n".format(t1 = t1, t2 = t2,\
			t3 = t3, t4=t4, t5=t5, t6=t6, t7=t7)
		f.write(line)"""

"""
#Experiment 1.2:
with open('Experiment_1.2_results.txt', 'w') as f:
	for file in os.listdir(directory):
		t1 = read_one_char(directory+file, 1000)
		t2 = read_one_line(directory+file, 1000)
		t3 = read_bytes(directory+file, 10, 1000)
		t4 = read_bytes(directory+file, 100, 1000)
		t5 = read_bytes(directory+file, 1000, 1000)
		t6 = read_bytes(directory+file, 10000, 1000)
		t7 = read_with_memory_mapping(directory+file, 1000)
		line = "{t1:.2}, {t2:.2}, {t3:.2}, {t4:.2}, {t5:.2}, {t6:.2}, {t7:.2}\n".format(t1 = t1, t2 = t2,\
				t3 = t3, t4=t4, t5=t5, t6=t6, t7=t7)
		f.write(line)"""
#Experiment 1.3:
@timerfunc
def rrmerge_buffer_buffer(filenames, output_filename, block_size):
	with open(output_filename, 'w', buffering = block_size) as outfile:
		with ExitStack() as stack:
			files = [stack.enter_context(open(directory+fname, 'r', buffering = 1)) for fname in filenames]
			flag = 1 #True if at least one file has some contect to be read
			while flag > 0:
				flag = 0
				for file in files:
					line = file.readline()
					if line != '':
						outfile.write(line) 
						flag = flag + 1
@timerfunc
def rrmerge_buffer_mmap(filenames, output_filename):

	#calculate the total size required of output file
	total_size = 0
	for file in filenames:
		with open(directory+file, 'r') as f:
			f.seek(0,2)
			total_size = total_size + f.tell()

	print("Total size of output file: ", total_size)

	#Extend the size of output file to total_size
	with open(output_filename) as f: #create a file if it doesn't exist
		pass
	os.truncate(output_filename, total_size)

	#Write to the file with memory mapping
	with open(output_filename, 'r+') as f:
		with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE) as outfile:
			with ExitStack() as stack:
				files = [stack.enter_context(open(directory+fname, 'r', buffering = 1)) for fname in filenames]
				flag = len(files) #how many files some content to be read
				while flag > 0:
					flag = 0
					for file in files:
						line = file.readline()
						if line != '':
							outfile.write(line.encode())
							flag = flag + 1
@timerfunc
def rrmerge_mmap_buffer(filenames, output_filename, block_size):
	with open(output_filename, 'w', buffering = block_size) as outfile:
		with ExitStack() as stack:
			files = [stack.enter_context(open(directory+fname, 'r')) for fname in filenames]
			mapped_files = [stack.enter_context(mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)) for file in files]
			flag = len(files)
			while flag > 0:
				flag = 0
				for file in mapped_files:
					line = file.readline()
					if line != b'':
						outfile.write(line.decode('utf-8'))
						flag = flag + 1

@timerfunc
def rrmerge_mmap_mmap(filenames, output_filename):
	#calculate the total size required of output file
	total_size = 0
	for file in filenames:
		with open(directory+file, 'r') as f:
			f.seek(0,2)
			total_size = total_size + f.tell()

	print("Total size of output file: ", total_size)

	#Extend the size of output file to total_size
	with open(output_filename) as f: #create a file if it doesn't exist
		pass
	os.truncate(output_filename, total_size)

	#Write with memory mapping
	with open(output_filename, 'r+') as f:
		with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE) as outfile:
			with ExitStack() as stack:
				files = [stack.enter_context(open(directory+fname, 'r')) for fname in filenames]
				mapped_files = [stack.enter_context(mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)) for file in files]
				flag = len(files)
				while flag > 0:
					flag = 0
					for file in mapped_files:
						line = file.readline()
						if line != b'':
							outfile.write(line)
							flag = flag + 1

#rrmerge_line_input_buffer_output(os.listdir(directory), 'Experiment_1.3_line_output.txt', 1)
#rrmerge_line_input_buffer_output(os.listdir(directory), 'Experiment_1.3_1000_bytes.txt', 1000)
print("Line to 10K Buffer:")
rrmerge_buffer_buffer(os.listdir(directory), 'Experiment_1.3_10000_bytes.txt', 10000)
print("Line to 100K Buffer:")
rrmerge_buffer_buffer(os.listdir(directory), 'Experiment_1.3_100000_bytes.txt', 100000)
print("Line to mmap:")
rrmerge_buffer_mmap(os.listdir(directory), 'Experiment_1.3_line_mmap.txt')
print("mmap to 1K Buffer:")
rrmerge_mmap_buffer(os.listdir(directory), 'mmap_buffer.txt',1000)
print("mmap to 10K Buffer:")
rrmerge_mmap_buffer(os.listdir(directory), 'mmap_buffer.txt',10000)
print("mmap to 100K Buffer:")
rrmerge_mmap_buffer(os.listdir(directory), 'mmap_buffer.txt',100000)
print("mmap to mmap Buffer:")
rrmerge_mmap_mmap(os.listdir(directory), 'mmap_mmap.txt')







