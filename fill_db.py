from cassandra.cluster import Cluster
import csv
import os
import sys
import pandas as pd


os_path = os.path
sys_exit = sys.exit


if __name__ == '__main__':
    cluster = Cluster()
    session = cluster.connect('reviews')
    file_path = input('Input path to .tsv file: ')

    if not os_path.isfile(file_path) or not file_path.endswith('.tsv'):
        print('You must input path to .tsv file for filling db.')
        sys_exit()

    skipped_rows = 0
    df = pd.read_csv(file_path, sep='\t', header=0)

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

    for index, row in df.iterrows():
        if len(row) != 15:
            skipped_rows += 1
            continue
        year_month = '-'.join(row[14].split('-')[:2])
        #session.execute(pStatement_1, [row[3], row[2]])
        #session.execute(pStatement_2, [row[3], row[7], row[2]])
        #session.execute(pStatement_3, [row[1], row[2]])
        #session.execute(pStatement_4, [year_month, row[14], row[3], row[2]])
        #session.execute(pStatement_5, [year_month, row[11], row[14], row[1], row[2]])
        session.execute(pStatement_7_8, [year_month, row[7], row[14], row[1], row[2]])

        if index % 10000 == 0:
            print("Row processed: {}".format(index))
