import csv
import json
import sys
import numpy as np
import math
from matplotlib import pyplot as plt


def load_config():
    with open('charts.cfg') as data_file:
        data = json.load(data_file)
    return data


def chart(file_name, col, title, xlabel, ylabel):
    cfg = load_config()
    ycsb_results_location = cfg["ycsb_results_location"]
    data = []
    with open(ycsb_results_location + file_name, 'r') as csvfile:
        rows = csv.reader(csvfile, delimiter=';')
        for i in range(1,len(rows)):
                data.append(int(rows[i]))

    bins = np.linspace(math.ceil(min(data)),
                       math.floor(max(data)),
                       5)  # fixed number of bins

    plt.xlim([min(data) - 5, max(data) + 5])

    plt.hist(data, bins=bins, alpha=0.5)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    plt.show()


def main(arg):
    file_name = arg[0]
    col = arg[1]
    title = arg[2]
    xlabel = arg[3]
    ylabel = arg[4]
    chart(file_name, col, title, xlabel, ylabel)


if __name__ == "__main__":
    main(sys.argv[1:])
