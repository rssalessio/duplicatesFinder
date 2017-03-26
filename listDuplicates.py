## listDuplicates
## Author: Alessio Russo, russo.alessio@outlook.com

import collections
from collections import deque
import hashlib
import multiprocessing as mp
import os
from sets import Set
import sys
import threading as thread
import time
import argparse


class Args:
	root_folder = None
	enable_threading = False
	ignored_directories = None
	duplicates_list = None
	thread_lock = None
	thread_end = None

parser = argparse.ArgumentParser(description='Python script to find files with the same content.')
parser.add_argument('dir', help='Root direcory.')
parser.add_argument("-i", "--ignore", metavar='I', help="Reads from I the list of directories that don't have to be processed")
parser.add_argument("-t", "--threads", help="Enables threading.",
                    action="store_true")

bytes_processed = 0.0


## Print files with same hash digest, ask the user which files he/she wants to delete
def printDuplicates(duplicates_list):
	it = 1
	for digest in duplicates_list:
		while (len(duplicates_list[digest]) > 1) : 
			print '-------------\n Digest n.', it
			j = 0
			for file in duplicates_list[digest]:
				print '\t [', j, '] ', file
				j += 1
			try:
				usr_in = input('Which file do you wanna delete (-1 to continue or enter): ')
			except SyntaxError:
				break;
			if (usr_in == -1):
				break
			if (usr_in < 0) or (usr_in > j-1):
				print 'Wrong input!'
			else:
				os.remove(duplicates_list[digest][usr_in])
				duplicates_list[digest].pop(usr_in)
				print('Removed.')
		it = it + 1

## Hash calculation
def calculateHash(file_path):
	BLOCKSIZE = 65536
	size = 0
	hasher =  hashlib.sha1()
	try:
		with open(file_path) as fl:
			buf = fl.read(BLOCKSIZE)
			while len(buf) > 0:
				size += len(buf)
				hasher.update(buf)
				buf = fl.read(BLOCKSIZE)
	except IOError:
		size = 0
	return hasher.hexdigest(), size


## File analyzer
def checkFiles(root, files, params) :
	global bytes_processed
	if (len(files) > 0) :
		for name in files :
			if not name.startswith('.'):
				full_path = os.path.join(root,name)
				digest, size = calculateHash(full_path)
				params.thread_lock.acquire()
				params.duplicates_list[digest].append(full_path)
				bytes_processed += size/(1024 * 1024)
				params.thread_lock.release()

## Thread routine
def threadRoutine(params, buf):
	while params.thread_end.isSet() == False or len(buf) > 0 :
		try:
			if (len(buf) > 0):
				data =  buf.pop()
				if (len(data) > 0):
					root = data[0]
					files  = data[1]
					if (len(files) > 0):
						checkFiles(root, files, params)
			else:
				time.sleep(0.01)
		except IndexError:
			continue

## Print statistics function
def printStats(params):
	cycle_time =  time.time()
	while params.thread_end.isSet() == False:
		if  time.time()-cycle_time > 0.5:
			print '\rProcessed ', bytes_processed, 'MB',
			sys.stdout.flush()
			cycle_time = time.time()
		time.sleep(0.5)

def main(params) :
	start_time = time.time()
	n_cpu = mp.cpu_count()*2

	params.thread_lock = thread.Lock()
	params.thread_end = thread.Event()

	threads = []

	## Start threads
	if params.enable_threading:
		buf =  deque()
		for i in range(n_cpu):
			t = thread.Thread(target=threadRoutine, args=(params, buf))
			threads.append(t)
			t.start()

	## Console output thread
	t = thread.Thread(target=printStats, args=(params,))
	threads.append(t)
	t.start()

	## Walk
	for root, dirs, files in os.walk(params.root_foolder) :
		for directory in dirs:
			# Ignote temporary directories
			if directory.startswith('.'):
				dirs.remove(directory)
			# Ignore directories in the ignore file
			if (params.ignored_directories != None):
				temp = os.path.join(root, directory)
				if temp in params.ignored_directories:
					dirs.remove(directory)
					params.ignored_directories.remove(temp)
		# add work to threads
		if (params.enable_threading) and len(files)>0 : 
			buf.appendleft([root,files])
		elif params.enable_threading == False and len(files)>0 :
			checkFiles(root, files, params)
		else:
			time.sleep(0.01)

	#end all Threads
	params.thread_end.set()
	while len(threads) > 0 :
		time.sleep(0.1)
		for t in threads :
				if (t.isAlive() == False):
					threads.remove(t)
	
	print '\rMB processed ', bytes_processed
	print 'Time elapsed: ', time.time()-start_time
	print 'Average velocity [MB/Sec]: ', bytes_processed/(time.time()-start_time)
	printDuplicates(params.duplicates_list)

def directoriesToIgnore(fileName):
	directories = []
	if (fileName != None) :
		try:
			with open(fileName,'r') as f:
				directories = f.read().splitlines()
		except IOError:
			pass

	if (fileName == None or len(directories) == 0):
		return None
	else:
		return set(directories)

if __name__ == '__main__':
	## Parse arguments
	args = parser.parse_args()
	params = Args()

	## Set parameters
	#Set absolute path
	params.root_foolder = os.path.abspath(args.dir)
	params.ignored_directories = directoriesToIgnore(args.ignore)
	params.enable_threading = args.threads
	params.duplicates_list = collections.defaultdict(list)
	main(params)
