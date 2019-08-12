# CLI execution guide

After successfully install and configurated. User's can test by using CLI commands.py "test_function" ,e.g., "python ./commands.py test_function

In case user want to run Map function:
Simply edit "map_function" method inside commands.py then execute commands "python ./commands.py map --input ['python list using space to separate input for each worker ,e.g., --input S3key1/object1 S3key1/object2 S3key2/object1'] --instance_type '(small/medium/large)' --s3_file_url (True/False)"
Note. instance_type use to indicate size of the worker that execute the task and s3_file_url indicate that passed input is list of S3 key not common python object.

In case user want to run MapReduce function
Similar to Map, Simply edit "reduce_function" method inside commands.py then execute commands "python ./commands.py map_reduce --input [**Job's input**] --instance_type --instance_type '(small/medium/large)' --s3_file_url (True/False)"

## Authors

* **Pichaya Kanjanapisith** - *Initial work* - [PurpleBooth](https://github.com/Pkanjan37/setuLightwight)

**Jörn Kulenkamp**  - *Project advisor*

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details


