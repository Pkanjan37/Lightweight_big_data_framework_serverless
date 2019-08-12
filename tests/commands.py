import click
import pywren

def hello_world(x):
        return "Hello world"
def map_function(x):
     """
    Define your map function here
    """
def reduce_function(x):
     """
    Define your reduce function here
    """
@click.group()
def cli():
    pass

@cli.command()
def cmd1():
    print("eiei")

@cli.command()
def test_function():
    """
    Simple single-function test
    """
    
    input = [1]
    wrenexec = pywren.default_executor()
    fut = wrenexec.map(hello_world, input)
    res = fut.result_state()

    click.echo("function returned: {}".format(res))

@cli.command()
@click.option('--input', default=None, type=list,
              help='Input for the jobs, using space to separate list example input=key1/object1 k2/object2')
@click.option('--instance_type', default="small", type=str,
              help='Instance type of the worker')
@click.option('--s3_file_url', default=False, type=bool,
              help='True if input is S3 key')
def map(input,instance_type,s3_file_url):
    """
    Simple Map-function test
    """
    print(input)
    print(instance_type)
    print(s3_file_url)
    
    # if(input == None):
    #     raise Exception("Please assgin the input")
    inputWithoutSpace = []
    for x in input:
        if x!=' ':
            inputWithoutSpace.append(x)
    print(inputWithoutSpace)
    if (instance_type != "small" or instance_type != "medium" or instance_type != "large"):
        raise Exception("Work instance type assign, it should be ('small','medium','large')")
    wrenexec = pywren.default_executor()
    fut = wrenexec.map(map_function, inputWithoutSpace,instance_specify=instance_type,s3_file_url=s3_file_url)
    res = fut.result_state()

    click.echo("function returned: {}".format(res))

@cli.command()
@click.option('--input', default=None, type=list,
              help='Input for the jobs')
@click.option('--instance_type', default="small", type=str,
              help='Instance type of the worker')
@click.option('--s3_file_url', default=False, type=bool,
              help='True if input is S3 key')
def map_reduce(input,instance_type,s3_file_url):
    """
    Simple MapReduce-function test
    """
    
    if(input == None):
        raise Exception("Please assgin the input")
    inputWithoutSpace = []
    for x in input:
        if x!=' ':
            inputWithoutSpace.append(x)
    if (instance_type != "small" or instance_type != "medium" or instance_type != "large"):
        raise Exception("Work instance type assign, it should be ('small','medium','large')")
    wrenexec = pywren.default_executor()
    fut = wrenexec.map(map_function, inputWithoutSpace,instance_specify=instance_type,s3_file_url=s3_file_url)
    futures = wrenexec.reducer(reduce_function,fut,instance_specify=instance_type)
    res = fut.result_state()

    click.echo("function returned: {}".format(res))

cli = click.CommandCollection(sources=[cli])

if __name__ == '__main__':
    cli()
