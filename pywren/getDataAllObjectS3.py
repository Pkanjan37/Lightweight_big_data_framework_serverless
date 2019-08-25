import boto3

s3obj = boto3.client('s3')
listObjKey = []
repoList = ['MOCK2/0/','MOCK2/1/']
for k in repoList:
                    # print(k)    
        result = s3obj.list_objects_v2(Bucket='output-bucky', Prefix=k)
        for i in result['Contents']:

            # print(i['Key'])
            listObjKey.append(i['Key'])

print(listObjKey)