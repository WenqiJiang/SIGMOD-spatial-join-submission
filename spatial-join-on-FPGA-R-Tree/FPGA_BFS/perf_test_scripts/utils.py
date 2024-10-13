import os
import re

def assert_keywords_in_file(fname, keyword):
	"""
	Given an input text file, assert there is at least a line that contains a keyword
	"""
	with open(fname) as f:
		lines = f.readlines()
		for line in lines:
			if keyword in line:
				return True
	return False

def get_number_file_with_keywords(fname, keyword, dtype='int'):
	"""
	Given an input text file, find the line that contains a keyword,
		and extract the first number (int or float)
	"""
	assert dtype == 'int' or dtype == 'float'
	if dtype == 'int':
		pattern = r"\d+"
	elif dtype == 'float':
		pattern = r"(\d+.\d+)"

	result = None
	with open(fname) as f:
		lines = f.readlines()
		for line in lines:
			if keyword in line:
				result = re.findall(pattern, line)[0]
	assert result is not None
	if dtype == 'int':
		return int(result)
	elif dtype == 'float':
		return float(result)
