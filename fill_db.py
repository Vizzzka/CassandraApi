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

    strCQL_4 = "INSERT INTO reviews.item_reviews_by_date (year_month, review_date, item_id, review_id)" \
               " VALUES (?,?,?,?);"
    pStatement_4 = session.prepare(strCQL_4)

    for index, row in df.iterrows():
        if len(row) != 15 or index == 0:
            skipped_rows += 1
            continue
        #session.execute(pStatement_1, [row[3], row[2]])
        #session.execute(pStatement_2, [row[3], row[7], row[2]])
        #session.execute(pStatement_3, [row[1], row[2]])
        year_month = row[14]    #change
        session.execute(pStatement_4, [year_month, row[14], row[3], row[2]])

        if index % 10000 == 0:
            print("Row processed: {}".format(index))
