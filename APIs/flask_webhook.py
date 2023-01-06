from flask import Flask, request, json


# TOKEN = jbPScEk9SVSE8QvkJsU9eLR69UWFXb5d
app = Flask(__name__)


@app.route('/')
def hello():
    return 'Webhooks with Python'


@app.route('/receive_data', methods=['POST'])
def receive_data():
    data = request.json
    print(data)
    print(f"Event: {data['event']['type']}")
    print(f"Property: {data['event']['payload']['propertiesId']}")
    print(f"Arriving: {data['event']['payload']['arrivalDate']}")

    return data


if __name__ == '__main__':
    app.run(debug=True)
