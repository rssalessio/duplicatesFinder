import collections
from collections import deque
import hashlib
import multiprocessing as mp
import os
from sets import Set
import sys
import threading as thread
import time

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

def checkFiles(root, files, lock, dlist) :
	bytes_processed = 0.0
	if (len(files) > 0) :
		for name in files :
			if not name.startswith('.'):
				full_path = os.path.join(root,name)
				digest, size = calculateHash(full_path)
				lock.acquire()
				duplicates_list[digest].append(full_path)
				lock.release()
				bytes_processed += size/(1024 * 1024)
	return bytes_processed

def threadRoutine(end, lock, buf, dlist) :
	bytes_processed = 0
	while end.isSet() == False or len(buf) > 0 :
		try:
			if (len(buf) > 0):
				data =  buf.pop()
				if (len(data) > 0) :
					root = data[0]
					files  = data[1]
					if (len(files) > 0):
						bytes_processed += checkFiles(root, files, lock, dlist)
		except IndexError:
			continue
		time.sleep(0.01)
	print 'MB processed ', bytes_processed

def main(argv,duplicates_list) :
	start_time = time.time()
	n_cpu = mp.cpu_count()
	threads = []
	parent_folder = argv[0]
	lock = thread.Lock()
	end = thread.Event()
	bytes_processed = 0
	print_step = 0

	if (len(argv) > 1):
		enable_threads = True
		buf =  deque()
	else:
		enable_threads = False

	if enable_threads :
		for i in range(n_cpu):
			t = thread.Thread(target=threadRoutine, args=(end, lock, buf, duplicates_list))
			threads.append(t)
			t.start()

	for root, dirs, files in os.walk(parent_folder) :
		if print_step % 100 == 0 :
			print '\rProcessed ', bytes_processed, 'MB',
			sys.stdout.flush()
		for directory in dirs:
			if directory.startswith('.'):
				dirs.remove(directory)
		if (enable_threads) and len(files)>0 : 
			buf.appendleft([root,files])
		elif enable_threads == False and len(files)>0 :
			bytes_processed += checkFiles(root, files, lock, duplicates_list)
		print_step += 1

	end.set()
	while len(threads) > 0 :
		time.sleep(0.1)
		for t in threads :
				if (t.isAlive() == False):
					threads.remove(t)
	
	print '\rMB processed ', bytes_processed
	print 'Time elapsed: ', time.time()-start_time
	print 'Average velocity [MB/Sec]: ', bytes_processed/(time.time()-start_time)
	printDuplicates(duplicates_list)

if __name__ == '__main__':
	duplicates_list = collections.defaultdict(list)
	main(sys.argv[1:], duplicates_list)
