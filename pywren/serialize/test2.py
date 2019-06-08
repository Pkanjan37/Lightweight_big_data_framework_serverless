# import sys
# sys.path.append('C:\\Users\\xifer\\LightWeightServerlessBigData\\playground\\botocore\\test.py')
# import test
# print(test)
# test.folder_explorer()

import numpy as np
import json
import jsonpickle
a = np.arange(4).reshape(2, 2)
b = np.array([[11,12],[13,14]]) 
print(np.dot(a,b))

frozen = jsonpickle.encode(a)
frozen2= jsonpickle.encode(b)
output_data = {'output':frozen}
output_data2 = {'output':frozen2}
# print(output_data)
a1 = json.dumps(output_data)
b1 = json.dumps(output_data2)
# print(b)
# jsson ="{\"py/reduce\": [{\"py/function\": \"numpy.core.multiarray._reconstruct\"}, {\"py/tuple\": [{\"py/type\": \"numpy.ndarray\"}, {\"py/tuple\": [0]}, {\"py/b64\": \"Yg==\\n\"}]}, {\"py/tuple\": [1, {\"py/tuple\": [3, 5]}, {\"py/reduce\": [{\"py/type\": \"numpy.dtype\"}, {\"py/tuple\": [\"i4\", 0, 1]}, {\"py/tuple\": [3, \"<\", null, null, null, -1, -1, 0]}, null, null]}, false, {\"py/b64\": \"AAAAAAEAAAACAAAAAwAAAAQAAAAFAAAABgAAAAcAAAAIAAAACQAAAAoAAAALAAAADAAAAA0AAAAO\\nAAAA\\n\"}]}, null, null]}"
c1 = json.loads(a1)
c2 = json.loads(b1)
final_result1 = jsonpickle.decode(c1['output'])
final_result2 = jsonpickle.decode(c2['output'])
print(np.dot(final_result1,final_result2))
# thawed = jsonpickle.decode(jsson)
# print(thawed)