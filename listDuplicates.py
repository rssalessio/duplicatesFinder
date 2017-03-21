import collections
import hashlib
import os
from sets import Set
import sys

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
	hasher =  hashlib.sha1()
	try:
		with open(file_path) as fl:
			buf = fl.read(BLOCKSIZE)
			while len(buf) > 0:
				hasher.update(buf)
				buf = fl.read(BLOCKSIZE)
	except IOError:
		print 'Can\'t open ', file_path
	return hasher.hexdigest()

def main(argv,duplicates_list) :
	for root, dirs, files in os.walk(argv[0]) :
		for directory in dirs:
			if directory.startswith('.'):
				dirs.remove(directory)
		print 'Current folder: ', root
		for name in files :
			if not name.startswith('.'):
				full_path = os.path.join(root,name)
				digest = calculateHash(full_path)
				duplicates_list[digest].append(full_path)
	printDuplicates(duplicates_list)

if __name__ == '__main__':
	duplicates_list = collections.defaultdict(list)
	main(sys.argv[1:], duplicates_list)