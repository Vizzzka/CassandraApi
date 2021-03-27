from flask import Flask
from flask import request
from flask import jsonify

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


# Q1 -Q2  /products/B00EFDZ288/reviews or  /products/B00EFDZ288/reviews?star_rating=5
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


#Q3 /customers/47293563/reviews
@app.route('/customers/<customer_id>/reviews')
def reviews_by_customer_id(customer_id):
    rows = session.execute("SELECT JSON review_id FROM reviews.reviews_by_customer_id "
                           "WHERE customer_id = {};".format(customer_id))

    rows = [row for row in rows]
    return jsonify(rows)


#Q4 /products?sorted_by_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
@app.route('/products')
def most_reviewed_items_in_date_range():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sorted_by_reviews = request.args.get('sorted_by_reviews')
    limit = request.args.get('limit')


#Q5, Q7-Q8
# Q5 /customers?sorted_by_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
# Q7 /customers?sorted_by_negative_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
# Q8 /customers?sorted_by_positive_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
@app.route('/customers')
def most_productive_customers_in_date_range():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sorted_by_reviews = request.args.get('sorted_by_reviews')
    sorted_by_negative_reviews = request.args.get('sorted_by_negative_reviews')
    sorted_by_positive_reviews = request.args.get('sorted_by_positive_reviews')
    limit = request.args.get('limit')


if __name__ == '__main__':
    cluster = Cluster()
    session = cluster.connect('reviews')
    app.run()



