from flask import Flask
from flask import request
from flask import jsonify

import json
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider


app = Flask(__name__)


@app.route('/')
def home():
    rows = session.execute("SELECT JSON COUNT (*) FROM reviews.reviews_by_product_id").one()
    rows = [row for row in rows]
    return jsonify(rows)


# Q1 -Q2
@app.route('/products/<product_id>/reviews')
def reviews_by_product_id(product_id):
    star_rating = request.args.get('star_rating')
    if star_rating is None:
        rows = session.execute("SELECT JSON review_id FROM reviews.reviews_by_product_id "
                           "WHERE product_id = '{}';".format(product_id))
    else:
        rows = session.execute("SELECT JSON review_id FROM reviews.reviews_by_star_rating_and_product_id "
                               "WHERE product_id = '{}' AND star_rating = {};".format(product_id, star_rating))
    rows = [row for row in rows]
    return jsonify(rows)

#Q3
@app.route('/customers/<customer_id>/reviews')
def reviews_by_customer_id(customer_id):
    rows = session.execute("SELECT JSON review_id FROM reviews.reviews_by_customer_id "
                           "WHERE customer_id = {};".format(customer_id))

    rows = [row for row in rows]
    return jsonify(rows)

#Q4


if __name__ == '__main__':
    cluster = Cluster()
    session = cluster.connect('reviews')
    app.run()



