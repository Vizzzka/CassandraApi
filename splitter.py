#!/usr/bin/env python3

import csv
import os
import sys


os_path = os.path
csv_writer = csv.writer
sys_exit = sys.exit


if __name__ == '__main__':
                
    try:
        chunk_size = int(input('Input number of rows of one chunk file: '))
    except ValueError:
        print('Number of rows must be integer. Close.')
        sys_exit()

    file_path = input('Input path to .tsv file for splitting on chunks: ')

    if (
        not os_path.isfile(file_path) or
        not file_path.endswith('.tsv')
    ):
        print('You must input path to .tsv file for splitting.')
        sys_exit()

    file_name = os_path.splitext(file_path)[0]

    with open(file_path, 'r', newline='', encoding='utf-8') as tsv_file:

        chunk_file = None
        writer = None
        counter = 1
        reader = csv.reader(tsv_file, delimiter='\t', quotechar='\'')

        for index, chunk in enumerate(reader):

            if index % chunk_size == 0:

                if chunk_file is not None:
                    chunk_file.close()

                chunk_name = '{0}_{1}.tsv'.format(file_name, counter)
                chunk_file = open(chunk_name, 'w', newline='', encoding='utf-8')
                counter += 1
                writer = csv_writer(chunk_file, delimiter='\t', quotechar='\'')

                print('File "{}" complete.'.format(chunk_name))

            writer.writerow(chunk)