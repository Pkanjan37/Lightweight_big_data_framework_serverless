# Lightweight big data processing framework over FaaS Platform based on PyWren


PyWren -- it's like a mini condor, in the cloud, for often-complex calls. You can get up to 40 TFLOPS peak from AWS Lambda:

# Lightweight big data processing framework over FaaS Platform based on PyWren

Lightweight is a big data framework based on FaaS platform currently support only Amazon Web Service.

The framework simply apply your single thread program execute in FaaS environment parallelly.

This framework is build apart of the Thesis at Technische Universit ̃At BerlinWirtschaftsinformatik Information Systems Engineering (ISE) under Fakult ̃IV der TU Berlin
## Installation

1. Clone our git repository i.e., https://github.com/Pkanjan37/LightWeightServerlessBigData
2. Go inside the directory we cloned.
3. Execute command python setup.py install
4. Execute command lightweight-setup to set the configuration for lightweight runtime.

### Prerequisites
AWS account
AWS Command Line interface
Window 10 or Ubuntu 16+
Python  3.6 
boto3 1.9.101
jsonpickle
numpy

### Get started

#### Import
Since our implementation builds on top of Pywren, before using, users have to import Pywren apart from their execution procedure.

Same as trivial python job, the import must include all relevant library to execute the 

#### Input
Assigning input to the job could be done by using a list variable, for instance, input = [1,2,3,4]

#### Executing jobs
Define the desired function same as a trivial Python function

Note. The function that passes to execute restricts only one argument. if you want to have an extra argument, you can use wrapper function or pass an extra argument.

Create an instance of an framework executor i.e., exec = pywren.default_executor()

Execute the task calling map or reducer function with defined function and input i.e., futures = exec.map(my_map_function3 , input_data)

Getting result by calling result_state() function i.e., result = futures.result_state()


## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.


## Authors

* **Pichaya Kanjanapisith** - *Initial work* - [PurpleBooth](https://github.com/Pkanjan37/setuLightwight)

**Jörn Kulenkamp**  - *Project advisor*

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details


