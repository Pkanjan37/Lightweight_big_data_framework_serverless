import shutil
import os
import zipfile
import boto3
import time
from zipfile import ZipFile
import jsonpickle
import importlib
import pywren
import glob
from pywren.storage import storage, storage_utils
# def copytree(src, dst, symlinks=False, ignore=None):
#     for item in os.listdir(src):
#         s = os.path.join(src, item)
        
#         d = os.path.join(dst, item)
#         print(s)
#         print(d)
#         # if os.path.isdir(s):
#         #     shutil.copytree(s, d, symlinks, ignore)
#         # else:
#         #     shutil.copy2(s, d)
def downloader(package):
    packagePath = os.path.dirname(pywren.__file__)
    print("package path<<<<<<<<",packagePath)
    print(packagePath+"\\serialize\\template"+"\\"+package)
    if not os.path.exists(packagePath+"\\serialize\\template"+"\\"+package):
        listWhl = glob.glob('*.whl')
        print("old file<<<<<<<<")
        print(listWhl)
        if len(listWhl)>0:
            for file in listWhl:
                os.remove(file)
        
        cmd = "pip download --only-binary=:all: --platform=manylinux1_x86_64 "+package
        os.system(cmd)
        # Create a ZipFile Object and load sample.zip in it
        listWhl = glob.glob('*.whl')
        print("download file<<<<<<<<",listWhl)
        if len(listWhl)>0:
            with ZipFile('.//'+listWhl[0], 'r') as zipObj:
        # Extract all the contents of zip file in different directory
                zipObj.extractall(packagePath+'//serialize//template')
def createLambdaFunction(funcN,zipName,config,storage_instance):
    fullZip = zipName+"\\"+funcN+".zip"
    upload_function(storage_instance,funcN,fullZip)
    lambda_client = boto3.client('lambda')
    storage_conf = storage_instance.get_storage_config_wrapped()
    print("Configure storage <<<<<<<<<<<<<<<<<<<<<<<<")
    print(storage_conf)
    input_bucket = storage_conf['bucket']
    print("Configure storage2 <<<<<<<<<<<<<<<<<<<<<<<<")
    print(input_bucket)
    AWS_LAMBDA_ROLE = config['account']['aws_lambda_role']
    AWS_ACCOUNT_ID = config['account']['aws_account_id']
    ROLE = "arn:aws:iam::{}:role/{}".format(AWS_ACCOUNT_ID, AWS_LAMBDA_ROLE)
    fn_name1 = funcN+'-128'
    fn_name2 = funcN+'-1600'
    fn_name3 = funcN+'-2688'
    # fn_role = 'arn:aws:iam::251584899486:role/pywren_exec_role_1'
    #clean in case already created
    try:
        response = lambda_client.delete_function(
            FunctionName=fn_name1
        )
    except lambda_client.exceptions.ResourceNotFoundException as e:
        print("Function not exist : %s" % e)

    try:
        response = lambda_client.delete_function(
            FunctionName=fn_name2
        )
    except lambda_client.exceptions.ResourceNotFoundException as e:
        print("Function not exist : %s" % e)

    try:
        response = lambda_client.delete_function(
            FunctionName=fn_name3
        )
    except lambda_client.exceptions.ResourceNotFoundException as e:
        print("Function not exist : %s" % e)
    
    create_lambda_func(lambda_client,fn_name1,ROLE,funcN,input_bucket,128)
    create_lambda_func(lambda_client,fn_name2,ROLE,funcN,input_bucket,1600)
    create_lambda_func(lambda_client,fn_name3,ROLE,funcN,input_bucket,2688)

def upload_function(storage_instance,FuncN,zipFile):
    response = storage_instance.put_data("functions/"+FuncN+".zip",open(zipFile, 'rb'))
def create_lambda_func(lambda_client,name,role,func,bucket,memSize):
    lambda_client.create_function(
        FunctionName=name,
        Runtime='python3.6',
        Role=role,
        Timeout=900,
        MemorySize=memSize,
        Handler=func+".lambda_handler",
        Code={'S3Bucket': bucket,'S3Key':'functions/'+func+'.zip' },
    )
# def givePermission(func):
#     client = boto3.client('lambda')
#     statementId =func+"-"+str(time.time())
#     statementId = statementId.replace(".", "-")
#     response = client.add_permission(
#         Action='lambda:InvokeFunction',
#         FunctionName=func,
#         Principal='s3.amazonaws.com',
#         SourceAccount='251584899486',
#         SourceArn='arn:aws:s3:::test-boto3-thesis',
#         StatementId=statementId,
#     )
#     print(response)

def zipfile2(name,dir_name,dest_dir):
    # shutil.make_archive(name, 'zip', dir_name)
    if os.path.exists(dest_dir+"\\"+name+".zip"):
        os.remove(dest_dir+"\\"+name+".zip")
    else:
        print("The file does not exist")
    make_archiveWarp(name,dir_name,dest_dir)
def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        print("-----")
        print(root)
        print(dirs)
        print(files)
        for file in files:
            ziph.write(os.path.join(root, file))
def make_archiveWarp(nameN,source, destination):
        base = os.path.basename(destination)
        name = nameN
        format = 'zip'
        archive_from = source
        archive_to = os.path.basename(source.strip(os.sep))
        shutil.make_archive(name, format, archive_from)
        shutil.move('%s.%s'%(name,format), destination)

def sourceBuilder(path,funcN,dir_path):   
    funcName = funcN
    fileName = funcName+".py"
    f = open(path, "r")
    w = open(dir_path+"\\"+fileName, "w")
    indent = False
    argument=''
    w.write("import json\n")
    w.write("import boto3\n")
    w.write("import jsonpickle\n")
    w.write("import time\n")
    w.write("import signal\n")
    # w.write("import jsonpickle\n")
    for x in f:
        if "import" in x:
            if "pywren" in x:
                print(x+" not print")
            else:
                print("0")
                print(x)
                w.write(x)
        elif "def " in x:
            if funcName in x:
                tmp = x.split("(")[1]
                print("<<<<")
                print(tmp)
                argument = tmp.split(")")[0]
                print("<<<<<")
                print(argument)
            print("1")
            print(x)
            w.write(x)
            indent = True
        elif x.startswith(' ') and indent==True:
            print("2")
            print(x)
            w.write(x)
        elif indent==True:
            indent = False
    w.write("def lambda_handler(event, context):\n")
    w.write("   try:\n")
    w.write("       with Timeout(840):\n")    
    w.write("           s3 = boto3.client('s3')\n")
    w.write("           bucket_in= 'xifer-pywren-118'\n")
    w.write("           plusfile = event['input']\n")
    w.write("           r = s3.get_object(Bucket=bucket_in, Key=plusfile)\n")
    w.write("           input_data = r['Body'].read().decode()\n")
    w.write("           received_data = json.loads(input_data)\n")
    # w.write("    received_data = jsonpickle.decode(received_data)\n")
    w.write("           inputData = received_data['data']\n")
    w.write("           compute = "+funcName+"(inputData)\n")
    w.write("           bucket_out= 'output-bucky'\n")
    w.write("           compute = jsonpickle.encode(compute)\n")
    w.write("           output_data = {'output':compute}\n")
    w.write("           output_data = json.dumps(output_data)\n")
    w.write("           s3.put_object(Bucket=bucket_out, Key=plusfile, Body=output_data)\n")
    w.write("           return {\n")
    w.write("               'statusCode': 200,\n")
    w.write("           }\n")
    w.write("   except Timeout.Timeout:\n")
    w.write("       print ('Timeout')\n")
    w.write("class Timeout():\n")
    w.write("   class Timeout(Exception):\n")
    w.write("       pass\n")
    w.write("   def __init__(self, sec):\n")
    w.write("       self.sec = sec\n")
    w.write("   def __enter__(self):\n")
    w.write("       signal.signal(signal.SIGALRM, self.raise_timeout)\n")
    w.write("       signal.alarm(self.sec)\n")
    w.write("   def __exit__(self, *args):\n")
    w.write("       signal.alarm(0)\n")
    w.write("   def raise_timeout(self, *args):\n")
    w.write("       raise  TimeoutError('Execution time exceed the limit')\n")
    

def zipper(directory,path,func,conf,storage_instance):
        
        dir_path = os.path.dirname(os.path.realpath(__file__))
        print(dir_path)
        # zipPath = dir_path+'\\tmp'
        # try:
        #     shutil.rmtree(zipPath)
        # except OSError as e:
        #     print ("Error: %s - %s." % (e.filename, e.strerror))
        pathTmp = dir_path+'\\tmp' 
        print(pathTmp)
        print("path temp <<<<<<<<<<<<<<<<<<<<<<<<")
        print(os.path.exists(pathTmp))
        if os.path.exists(pathTmp):
            shutil.rmtree(pathTmp)
        os.makedirs(pathTmp)
        
        sourceBuilder(path,func,pathTmp)
        shutil.copytree(dir_path+'\\template\\site-packages',pathTmp)
        
        for i in directory:
            libDirectory = i.split("\\")
            libFolder = libDirectory[len(libDirectory)-1]
            if not i.endswith('pywren') and not i.endswith('.py'):
                # print(i)
                # print("-------")
                # print(path+'\\'+libFolder)
                moduleName = i.split("\\")
                tempModulePath = ".//serialize//template"+"//"+moduleName[len(moduleName)-1]
                print("Temp module Path <<<<<<<<<<<<<<<<<<<<<<<<<")
                print(tempModulePath)
                if os.path.exists(tempModulePath):
                    shutil.copytree(tempModulePath,pathTmp+'\\'+libFolder)
                    
                else:
                    shutil.copytree(i,pathTmp+'\\'+libFolder)
        # zipf = zipfile.ZipFile(func+'.zip', 'w', zipfile.ZIP_DEFLATED)
        # zipdir(zipPath,zipf)
        # zipf.close()
        zipfile2(func,pathTmp,dir_path)
        createLambdaFunction(func,dir_path,conf,storage_instance)
        # givePermission(func)
        
# sourceBuilder("C:\\Users\\xifer\\LightWeightServerlessBigData\\playground\\pywrenMapTest.py","my_map3","C:\\Users\\xifer\\LightWeightServerlessBigData\\playground")
# givePermission("printNP")

# myList = {'C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\lib\\site-packages\\pywren',
#      'C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\lib\\site-packages\\watchtower', 
#          'C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\lib\\site-packages\\colorama',
#               'C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\lib\\site-packages\\mpmath', 
#                   'C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\lib\\site-packages\\click', 
#                       'C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\lib\\site-packages\\sympy'}
# zipper(myList,"C:\\Users\\xifer\\LightWeightServerlessBigData\\playground\\pywrenMapTest.py","my_map_function3")

# with open("code.py", "w") as source_code:
#     source_code.write(red.dumps())