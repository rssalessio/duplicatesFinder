import collections
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
		print 'Can\'t open ', file_path
	return hasher.hexdigest(), size

def checkFiles(lock, files, root, duplicates_list) :
	bytes_processed = 0.0
	for name in files :
		if not name.startswith('.'):
			full_path = os.path.join(root,name)
			digest, size = calculateHash(full_path)
			lock.acquire()
			duplicates_list[digest].append(full_path)
			lock.release()
	#		bytes_processed += size/(1024 * 1024)
	#print 'MB processed ', bytes_processed

def main(argv,duplicates_list) :
	start_time = time.time()
	n_cpu = mp.cpu_count()*2
	threads = []
	parent_folder = argv[0]
	lock = thread.Lock()
	if (len(argv) > 1):
		enable_threads = True
	else:
		enable_threads = False
	for root, dirs, files in os.walk(parent_folder) :
		for directory in dirs:
			if directory.startswith('.'):
				dirs.remove(directory)
		for t in threads :
			if (t.isAlive() == False):
				threads.remove(t)
		if (len(threads) <  n_cpu) and (len(files) > 0) and enable_threads == True:
			t = thread.Thread(target=checkFiles, args=(lock, files, root, duplicates_list))
			threads.append(t)
			t.start()
		elif len(files) > 0:
			checkFiles(lock, files, root, duplicates_list)
	print 'waiting all threads now'
	while len(threads) > 0 :
		time.sleep(0.1)
		for t in threads :
				if (t.isAlive() == False):
					threads.remove(t)
	end_time = time.time()-start_time
	print end_time
	printDuplicates(duplicates_list)

if __name__ == '__main__':
	duplicates_list = collections.defaultdict(list)
	main(sys.argv[1:], duplicates_list)
