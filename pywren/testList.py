import os 
# os.listdir('/usr/local/lib/python3.6/dist-packages/lightweight-0.4.0-py3.6.egg/pywren/serialize/template/site-packages')
import shutil,errno


# try:
#     shutil.copytree('/home/p/.local/lib/python3.6/site-packages/dateutil', '/home/p/Documents/deleteSoon2')
# except OSError as exc: 
#     if exc.errno == errno.ENOTDIR:
#         shutil.copy('/home/p/.local/lib/python3.6/site-packages/dateutil',  '/home/p/Documents/deleteSoon')
#     else: raise


# i = "home/p/.local/lib/python3.6/site-packages/jsonpickle"
# libDirectory = i.split("\\")

# print(libDirectory)
def eiei():

    def abc():
        x = 1+2
        return x

    def foo():         
        #do something with args 
        # a = arg1 + arg2
        a=0
        abc()
        haha(foo)         
        return a  
    foo()

def haha(func):

    import inspect
    lines = inspect.getsource(func)
    print(lines)

eiei()