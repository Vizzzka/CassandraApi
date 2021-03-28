from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import os
import sys
import pandas as pd


os_path = os.path
sys_exit = sys.exit


if __name__ == '__main__':
    auth_provider = PlainTextAuthProvider(
    username='cassandra', password='Lt5PCbgaEaTe')
    cluster = Cluster(auth_provider=auth_provider)
    session = cluster.connect('reviews')
    file_path = input('Input path to .tsv file: ')

    if not os_path.isfile(file_path) or not file_path.endswith('.tsv'):
        print('You must input path to .tsv file for filling db.')
        sys_exit()

    skipped_rows = 0

    # preparing and executing my INSERT statement
    strCQL_1 = "INSERT INTO reviews.reviews_by_product_id (product_id, review_id)" \
               " VALUES (?,?);"
    pStatement_1 = session.prepare(strCQL_1)

    strCQL_2 = "INSERT INTO reviews.reviews_by_star_rating_and_product_id (product_id, star_rating, review_id)" \
               " VALUES (?,?, ?);"
    pStatement_2 = session.prepare(strCQL_2)

    strCQL_3 = "INSERT INTO reviews.reviews_by_customer_id (customer_id, review_id) VALUES (?,?);"
    pStatement_3 = session.prepare(strCQL_3)

    strCQL_4 = "INSERT INTO reviews.item_reviews_by_date (year_month, review_date, product_id, review_id)" \
               " VALUES (?,?,?,?);"
    pStatement_4 = session.prepare(strCQL_4)

    strCQL_5 = "INSERT INTO reviews.customer_reviews_by_date (year_month, verified_purchase, review_date," \
               " customer_id, review_id)" \
               " VALUES (?,?,?,?,?);"
    pStatement_5 = session.prepare(strCQL_5)

    strCQL_7_8 = "INSERT INTO reviews.customers_reviews_by_date_and_star_rating" \
                 " (year_month, star_rating, review_date, customer_id, review_id)" \
                 " VALUES (?,?,?,?,?);"
    pStatement_7_8 = session.prepare(strCQL_7_8)

    with open(file_path, 'r', newline='', encoding='utf-8') as tsv_file:
        reader = csv.reader(tsv_file, delimiter='\t', quotechar='\'')
        for index, row in enumerate(reader):
            if len(row) != 15:
                skipped_rows += 1
                continue
            year_month = '-'.join(row[14].split('-')[:2])
            try:
                star_rating = int(row[7])
                customer_id = int(row[1])
            except:
                continue
            session.execute(pStatement_1, [row[3], row[2]])
            session.execute(pStatement_2, [row[3], star_rating, row[2]])
            session.execute(pStatement_3, [customer_id, row[2]])
            session.execute(pStatement_4, [year_month, row[14], row[3], row[2]])
            session.execute(pStatement_5, [year_month, row[11], row[14], customer_id, row[2]])
            session.execute(pStatement_7_8, [year_month, star_rating, row[14], customer_id, row[2]])

            if index % 10000 == 0:
                print("Row processed: {}".format(index))
