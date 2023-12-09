from flask import Flask, request, Response, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import BigInteger
import jsonpickle
import os

app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://naveenaganesan:password@localhost/brandpulse'

DB_HOST = '34.74.206.172'
DB_PORT = '5432'
DB_NAME = 'brandpulse'
DB_USER = 'postgres'
DB_PASSWORD = 'test1234'

DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URL
db = SQLAlchemy(app)

class Customer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    mobile = db.Column(db.String(20))
    email = db.Column(db.String(100))

# Create the Flask application context
app.app_context().push()

@app.route('/')
def index():
    return render_template('add_customer.html')

@app.route('/customer', methods=['POST'])
def create_customer():
    # data = request.get_json()
    # customer_name = data['name']
    # mobile_number = data['mobile']
    # email = data['email']

    customer_name = request.form['name']
    mobile_number = request.form['mobile']
    email = request.form['email']

    customer = Customer(name = customer_name, mobile = mobile_number, email = email)

    db.session.add(customer)
    db.session.commit()

    # return Response(response=jsonpickle.encode({"message": "Customer added successfully!"}), 
    #             status=200, mimetype="application/json")
    return render_template('add_customer.html', message="Customer added successfully!")

if __name__ == '__main__':
    print("Starting server")
    db.create_all()
    # app.run(debug=True)
    app.run(host='0.0.0.0', port=80, debug=True)


