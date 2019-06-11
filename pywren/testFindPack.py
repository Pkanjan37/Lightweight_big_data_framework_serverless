# from setuptools import setup, find_packages
# print(find_packages())

from pathlib import PurePath
path = "C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\lib\\concurrent"
path_split = PurePath(path).parts
print(path_split)
