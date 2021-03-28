from flask import Flask
from flask import request
from flask import jsonify
import json


from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider



def next_month(month_year):
    year, month = map(int, month_year.split('-'))
    if month < 12:
        return '-'.join([str(year), (('0' + str(month + 1)) if len(str(month + 1)) == 1 else str(month + 1))])
    return '-'.join([str(year + 1), str('01')])

app = Flask(__name__)


@app.route('/')
def home():
    rows = session.execute("SELECT JSON COUNT (*) FROM reviews.reviews_by_product_id").one()
    rows = [row for row in rows]
    return jsonify(rows)


# Q1 -Q2   http://127.0.0.1:5000/products/B00EFDZ288/reviews or  http://127.0.0.1:5000/products/B00EFDZ288/reviews?star_rating=5
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


#Q3 http://127.0.0.1:5000/customers/47293563/reviews
@app.route('/customers/<customer_id>/reviews')
def reviews_by_customer_id(customer_id):
    rows = session.execute("SELECT JSON review_id FROM reviews.reviews_by_customer_id "
                           "WHERE customer_id = {};".format(customer_id))

    rows = [row for row in rows]
    return jsonify(rows)


#Q4 http://127.0.0.1:5000/products?sorted_by_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
@app.route('/products')
def most_reviewed_items_in_date_range():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sorted_by_reviews = request.args.get('sorted_by_reviews')
    limit = request.args.get('limit')
    limit = int(limit)

    items_reviews_dct = dict()
    start_month = '-'.join(start_date.split('-')[:2])
    end_month = '-'.join(end_date.split('-')[:2])

    while start_month != next_month(end_month):
        rows = session.execute("SELECT JSON product_id FROM reviews.item_reviews_by_date"
                                " WHERE year_month = '{}' AND review_date >= '{}' AND review_date <= '{}';".
                                format(start_month, start_date, end_date))
        start_month = next_month(start_month)
        for row in rows:
            d = json.loads(row[0])
            items_reviews_dct[d["product_id"]] = items_reviews_dct.get(d["product_id"], 0) + 1
    print("Sorting...")
    lst = [(k, v) for k, v in sorted(items_reviews_dct.items(), key=lambda item: item[1], reverse=True)]
    return jsonify(lst[:limit])

#Q5, Q7-Q8
# Q5 http://127.0.0.1:5000/customers?sorted_by_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
# Q7 http://127.0.0.1:5000/customers?sorted_by_negative_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
# Q8 http://127.0.0.1:5000/customers?sorted_by_positive_reviews=1&start_date=2013-08-01&end_date=2013-10-01&limit=20
@app.route('/customers')
def most_productive_customers_in_date_range():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sorted_by_reviews = request.args.get('sorted_by_reviews')
    sorted_by_negative_reviews = request.args.get('sorted_by_negative_reviews')
    sorted_by_positive_reviews = request.args.get('sorted_by_positive_reviews')
    limit = request.args.get('limit')
    limit = int(limit)

    customers_reviews_dct = dict()
    start_month = '-'.join(start_date.split('-')[:2])
    end_month = '-'.join(end_date.split('-')[:2])

    if sorted_by_reviews == "1":
        while start_month != next_month(end_month):
            rows = session.execute("SELECT JSON customer_id FROM reviews.customer_reviews_by_date"
                                   " WHERE year_month = '{}' AND review_date >= '{}' AND review_date <= '{}'"
                                   " AND verified_purchase = 'Y';".
                                   format(start_month, start_date, end_date))
            start_month = next_month(start_month)
            for row in rows:
                d = json.loads(row[0])
                customers_reviews_dct[d["customer_id"]] = customers_reviews_dct.get(d["customer_id"], 0) + 1
        print("Sorting...")
        lst = [(k, v) for k, v in sorted(customers_reviews_dct.items(), key=lambda item: item[1], reverse=True)]
        return jsonify(lst[:limit])

    if sorted_by_negative_reviews == "1":
        star_rating = (1, 2)
    else:
        star_rating = (4, 5)
    while start_month != next_month(end_month):
        rows = session.execute("SELECT JSON customer_id FROM reviews.customers_reviews_by_date_and_star_rating"
                               " WHERE year_month = '{}' AND star_rating = {}"
                               " AND review_date <= '{}' AND review_date >= '{}';".
                                format(start_month, star_rating[0], end_date, start_date))
        for row in rows:
            d = json.loads(row[0])
            customers_reviews_dct[d["customer_id"]] = customers_reviews_dct.get(d["customer_id"], 0) + 1

        rows = session.execute("SELECT JSON customer_id FROM reviews.customers_reviews_by_date_and_star_rating"
                               " WHERE year_month = '{}' AND star_rating = {}"
                               " AND review_date <= '{}' AND review_date >= '{}';".
                                format(start_month, star_rating[1], end_date, start_date))
        for row in rows:
            d = json.loads(row[0])
            customers_reviews_dct[d["customer_id"]] = customers_reviews_dct.get(d["customer_id"], 0) + 1

        start_month = next_month(start_month)
    print("Sorting...")
    lst = [(k, v) for k, v in sorted(customers_reviews_dct.items(), key=lambda item: item[1], reverse=True)]
    return jsonify(lst[:limit])


if __name__ == '__main__':
    cluster = Cluster()
    session = cluster.connect('reviews')
    app.run()



