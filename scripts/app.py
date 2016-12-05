#!flask/bin/python
from flask import Flask, jsonify, make_response, abort, request
import airline_queries
#import test

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web', 
        'done': False
    }
]


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
    return jsonify({'tasks': tasks})

@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
    if len(task) == 0:
        abort(404)
    return jsonify({'task': task[0]})

@app.route('/todo/api/v1.0/tasks/', methods=['POST'])
def create_task():
    if not request.json or not 'title' in request.json:
        abort(400)
    task = {
        'id': tasks[-1]['id'] + 1,
        'title': request.json['title'],
        'description': request.json.get('description', ""),
        'done': False
    }
    tasks.append(task)
    return jsonify({'task': task}), 201

@app.route('/delay_statistics/', methods=['GET'])
def get_delay_statistics():
    start_date = request.args.get('sd')
    end_date = request.args.get('ed')
    carrier = request.args.get('cr')
    origin_city = request.args.get('oc')
    destination_city = request.args.get('dc')
#    print request.args.get('sy')
#    print request.args.get('sm')
#    print request.args.get('ey')
#    print request.args.get('em')
#    print request.args.get('cr')
#    print request.args.get('oc')
#    print request.args.get('dc')
    return airline_queries.query_delay_statistics(start_date, end_date, carrier, origin_city, destination_city)


@app.route('/delay_carrier/', methods=['GET'])
def get_delay_carrier():
    start_date = request.args.get('sd')
    end_date = request.args.get('ed')
    origin_city = request.args.get('oc')
    destination_city = request.args.get('dc')
    return airline_queries.query_most_delay_by_carriers(start_date, end_date, origin_city, destination_city)



@app.route('/most_cancelled/', methods=['GET'])
def get_most_cancelled():
    start_date = request.args.get('sd')
    end_date = request.args.get('ed')
    origin_city = request.args.get('oc')
    destination_city = request.args.get('dc')
    return airline_queries.query_carriers_with_max_CF(start_date, end_date, origin_city, destination_city)


@app.route('/air_time/', methods=['GET'])
def get_air_time():
    start_date = request.args.get('sd')
    end_date = request.args.get('ed')
    return airline_queries.query_carriers_with_max_airtime(start_date, end_date)


if __name__ == '__main__':
    app.run(debug=True)
