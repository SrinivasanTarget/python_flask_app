from flask import Flask, request, make_response, jsonify

app = Flask(__name__)

port = 8080

users = [
    {
        'name': 'user1',
        'role': 'admin',
        'age': 30
    },
    {
        'name': 'user2',
        'role': 'member',
        'age': 22
    },
]

@app.route('/users')
def get_users():
    return jsonify(users)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=port)