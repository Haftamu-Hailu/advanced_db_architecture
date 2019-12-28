from time import time
import mmap
import contextlib

#Read one character at a time
def read_one_char(file_url):
	with open(file_url, 'r', buffering=0) as f:
		result = []
		char = f.read(1)
		while char != '\n':
			print(char, end="")
			char = f.read(1)
	
def write_one_char(filename, literal):
	with open(file_url, 'a', buffering=0) as f:
		f.write(literal)

#Read one line at a time
def read_one_line(file_url):
	with open(file_url, 'r', buffering=1) as f:
		for line in f:
			print(line, end ="")

def write_one_line(filename, line):
	with open(filename, 'a', buffering=1) as f:
		f.writeline(line)

#Read B bytes(and store them in buffer) at a time
def read_bytes(file_url, block_size):
	with open(file_url, "r", buffering=block_size) as f:
		block = f.read(block_size)
		while block != "":
			print(block, end="")
			block = f.read(block_size)

#Write B bytes
def write_bytes(file_url, block_size, literal):
	with open(file_url, 'a', buffering=block_size) as f:
		f.write(literal)

#Use memory mapping to read a file
def read_with_memory_mapping(file_url, block_size):
	with open(file_url, 'r') as f:
		with contextlib.closing(mmap.mmap(f.fileno(), block_size, access=mmap.ACCESS_READ)) as m:
			block = m.read(block_size)
			while block != b"":
				print(block.decode('utf-8'), end="") 
				block = m.read(block_size)
				
def write_with_memory_mapping(file_url, block_size, literal):
	with open(file_url, 'a') as f:
		with contextlib.closing(mmap.mmap(f.fileno(), block_size, access=mmap.ACCESS_READ)) as m:
			m.write(literal)




