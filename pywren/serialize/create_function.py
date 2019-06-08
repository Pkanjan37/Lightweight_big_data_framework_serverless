import boto3

def createLambdaFunction(funcN,zipName):
    lambda_client = boto3.client('lambda')

    fn_name = funcN
    fn_role = 'arn:aws:iam::251584899486:role/pywren_exec_role_1'

    lambda_client.create_function(
        FunctionName=fn_name,
        Runtime='python3.6',
        Role=fn_role,
        Handler=funcN+"."+funcN,
        Code={'ZipFile': open(zipName, 'rb').read(), },
    )

createLambdaFunction("my_map_function6","C:\\Users\\xifer\\AppData\\Local\\Programs\\Python\\Python36\\Lib\\site-packages\\pywren\\serialize\\my_map_function6.zip")