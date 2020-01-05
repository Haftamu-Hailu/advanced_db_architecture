import time
import mmap
import contextlib
import os
import random
from contextlib import ExitStack

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
	with open(output_filename, 'w') as f: #create a file if it doesn't exist
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
	with open(output_filename, 'w') as f: #create a file if it doesn't exist
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

directory = "./imdb/"
#rrmerge_line_input_buffer_output(os.listdir(directory), 'Experiment_1.3_line_output.txt', 1)
#rrmerge_line_input_buffer_output(os.listdir(directory), 'Experiment_1.3_1000_bytes.txt', 1000)
#print("Line to 10K Buffer:")
#rrmerge_buffer_buffer(os.listdir(directory), 'Experiment_1.3_10000_bytes.txt', 10000)
#print("Line to 100K Buffer:")
#rrmerge_buffer_buffer(os.listdir(directory), 'Experiment_1.3_100000_bytes.txt', 100000)
#print("Line to mmap:")
#rrmerge_buffer_mmap(os.listdir(directory), 'Experiment_1.3_line_mmap.txt')
#print("mmap to 1K Buffer:")
#rrmerge_mmap_buffer(os.listdir(directory), 'mmap_buffer.txt',1000)
#print("mmap to 10K Buffer:")
#rrmerge_mmap_buffer(os.listdir(directory), 'mmap_buffer.txt',10000)
#print("mmap to 100K Buffer:")
#rrmerge_mmap_buffer(os.listdir(directory), 'mmap_buffer.txt',100000)
print("mmap to mmap:")
rrmerge_mmap_mmap(os.listdir(directory), 'mmap_mmap.txt')


