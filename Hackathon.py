from flask import Flask
from flask import render_template
from flask import request
from flask import Response
from collections import defaultdict
import json

#1533052818326_140157

#get flask app object  
app = Flask(__name__)

# Global attributes used  
super_params = ['ERROR', 'WARN']
exception_params = ['error', 'except', 'fail', 'not found']

# to serve index file on startup
@app.route("/")
def appStarter():  
    return render_template('index.html')

# define rest api path 
@app.route('/getlogdata')
def getLogdata( ):
    args = request.args;
    print(args)
    print(args['appid'])
    parseLogByAppId(args['appid'])
    f = open(args['appid']+'_result.json')
    data = f.readlines()
    js = json.dumps(data)

    resp = Response(js, status=200, mimetype='application/json')
    return resp;



def parseLogByAppId(appId):
  
    with open('application_'+appId+'.log') as f:
        x = f.readlines()

    attempt_list = []

    for i, z in enumerate(x):
        if 'ApplicationAttemptId' in z:
            attempt_list.append(i)

    if attempt_list != []:
        x = x[max(attempt_list):]

    error_dict = read_Hadoop_log(x)
    line_numbers = []
    error_type = ''

    if error_dict['ERROR'] != []:
        for rand in exception_params:
            for err_line in error_dict['ERROR']:
                if rand in err_line.lower():
                    line_number = x.index(err_line)
                    line_numbers.append(line_number)
                    break
                else:
                    continue
                break

    elif error_dict['WARN'] != []:
        for rand in exception_params:
            for err_line in error_dict['WARN']:
                if rand in err_line.lower():
                    line_number = x.index(err_line)
                    line_numbers.append(line_number)
                    break
                else:
                    continue
                break
    else:
        for rand in exception_params:
            for err_line in error_dict[rand]:
                if rand in err_line.lower():
                    line_number = x.index(err_line)
                    line_numbers.append(line_number)
                    break
                else:
                    continue
                break

    line_numbers.sort()
    main_Error = x[line_numbers[0]]
    print(main_Error)
    print(line_numbers[0])

    linestart = 0
    lineend = 0

    for i in reversed(x[0:line_numbers[0] + 1]):
        if 'WARN' in i or 'INFO' in i or 'ERROR' in i:
            linestart = x.index(i)
            break

    for i in x[line_numbers[0] + 1:]:
        if 'WARN' in i or 'INFO' in i or 'ERROR' in i:
            lineend = x.index(i)
            break

    error_dict['Main_Error'] = x[linestart:lineend]
    print(error_dict['Main_Error'])

    New_Dict = dict((el, dict((e2, []) for e2 in exception_params)) for el in error_dict.keys())

    for i in error_dict.keys():
        for j in error_dict[i]:
            for param in exception_params:
                if param in j.lower():
                    New_Dict[i][param].append(j)

    with open(appId+'_result.json', 'w') as fp:
        json.dump(New_Dict, fp)
		
def read_Hadoop_log(log_array):
    y = dict((el,[]) for el in exception_params)
    y.update({'WARN': []})
    y.update({'ERROR': []})
    for param in exception_params[0:3]:
        for line in log_array:
            if param in line.lower() and 'info' not in line.lower():
                if 'WARN' in line:
                    y['WARN'].append(line)
                if 'ERROR' in line:
                    y['ERROR'].append(line)
                else:
                    y[param].append(line)
    return y
		
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080,debug=True)