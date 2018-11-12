from flask import Flask
from flask_restful import Resource, Api, abort, request, reqparse
import json
import sys

app = Flask(__name__)
api = Api(app)
parser = reqparse.RequestParser()

causes_of_death = []

class Task(Resource):

    def post(self):
        if not request.json:
            abort(400)
        post_data = request.json
        causes_of_death.append(post_data)
        return 201

    def get(self):
        get_data = {}
        get_data["num_entries"] = len(causes_of_death)
        get_data["entries"] = causes_of_death
        return 201, str(get_data)

api.add_resource(Task, '/api/v1/entries')

if __name__ == '__main__':
    port = 5000
    if len(sys.argv) > 1:
        port = sys.argv[1]
    app.run(port = port, debug = True)