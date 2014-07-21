import csv


def get_data(filename):
    with open(filename, 'rb') as csvfile:
        datareader =csv.reader(csvfile, delimiter=',')
        for row in datareader:
            print row

get_data('n_1000_d_2.csv') 