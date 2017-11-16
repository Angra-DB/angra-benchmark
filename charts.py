# -*- coding: utf-8 -*-
import csv
import json
import sys
import numpy as np
# import math
import pylab as pl
from matplotlib.backends.backend_pdf import PdfPages


def operations_in_totals(file_in):
    cfg = load_config()
    csvfile = open(cfg["ycsb_results_location"] + file_in, 'r')
    rows = csv.reader(csvfile, delimiter=';')
    operations_list = []
    for row in rows:
        if row[5] not in operations_list and row[5] != 'Operation':
            operations_list.append(row[5])
    return operations_list


def db_name(s):
    if s == "mongodb":
        return "MongoDB"
    elif s == "angra":
        return "Angra-DB"
    elif s == "mysql":
        return "MySQL"
    elif s == "couchdb":
        return "CouchDB"
    else:
        return s


def load_config():
    with open('charts.cfg') as data_file:
        data = json.load(data_file)
    return data


def line_charts(file_in, file_out, titles, x_label, y_label,
                data_collum_number, stage, operation=None):

    cfg = load_config()
    # ycsb_results_location = cfg["ycsb_results_location"]
    # file_name = 'overall.csv'
    databases = []
    executions = []
    data = []
    threads = []
    workloads = []
    stages = []

    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
    markers = ['o', 's', 'D', 'x', '^']

    csvfile = open(cfg["ycsb_results_location"] + file_in, 'r')
    rows = csv.reader(csvfile, delimiter=';')

    for row in rows:
        if row[0] not in executions and row[0] != 'Execution':
            executions.append(row[0])
        if row[1] not in databases and row[1] != 'Database':
            databases.append(row[1])
        if row[2] not in threads and row[2] != 'Threads':
            threads.append(row[2])
        if row[3] not in workloads and row[3] != 'Workload':
            workloads.append(row[3])
        if row[4] not in stages and row[4] != 'Stage':
            stages.append(row[4])

        data.append(row)
    csvfile.close()
    i = 0
    x = []
    y = []
    plots = []
    inserted_plots = {}
    ylim_max = 0
    y_partials = []

    pl.figure(figsize=(9, 9))
    for db in databases:
        for th in threads:
            x.append(th)
            for ex in executions:
                if operation is None:
                    base_list = [x1 for x1 in data
                                 if x1[0] == ex
                                 and x1[1] == db
                                 and x1[2] == th
                                 and x1[4] == stage]
                else:
                    base_list = [x1 for x1 in data
                                 if x1[0] == ex
                                 and x1[1] == db
                                 and x1[2] == th
                                 and x1[4] == stage
                                 and x1[5] == operation]

                for d in base_list:
                    y_partials.append(d[data_collum_number])
                    if float(d[data_collum_number]) > ylim_max:
                        ylim_max = float(d[data_collum_number])
            if not len(y_partials):
                media = 0
            else:
                media = sum([float(y1) for y1 in y_partials]) \
                    / len(y_partials)
            y.append(media)
            y_partials = []
        if [y2 for y2 in y if y2 != 0]:
            inserted_plots[str(i)] = pl.plot(x, y,
                                             colors[i] + '-' + markers[i],
                                             label=db_name(db))

            plots.append(inserted_plots[str(i)])
        i = i + 1
        x = []
        y = []
    if len(executions) > 1:
        pl.title(titles[1])
    else:
        pl.title(titles[0])
    pl.xlabel(x_label)
    pl.ylabel(y_label)
    pl.legend(loc='lower center',
              bbox_to_anchor=(1.15,  # horizontal
                              0.02),  # vertical
              ncol=1, numpoints=1)
    pl.xlim(0, max([float(x) for x in threads]) + 1)
    # pl.ylim(-0.1 * ylim_max, 1.1 * ylim_max)
    pl.xticks(tuple([float(x) for x in threads]))
    pl.tick_params(axis='both', which='both', bottom='off', top='off',
                   labelbottom='on', left='off', right='off',
                   labelleft='on')
    pl.gca().spines['top'].set_visible(False)
    pl.gca().spines['right'].set_visible(False)
    pl.grid()
    # pl.tight_layout()
    if plots:
        pdf = PdfPages(file_out)
        pl.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()
    # saves the current figure into a pdf page
    pl.close()


def histogram_charts(file_in, file_out, titles, x_label, y_label,
                     stage,
                     th, operation,
                     data_collum_number=6, data_collum_multiplier=7):

    cfg = load_config()
    # ycsb_results_location = cfg["ycsb_results_location"]
    # file_name = 'overall.csv'
    databases = []
    executions = []
    dat = []
    threads = []
    workloads = []
    stages = []

    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
    markers = ['o', 's', 'D', 'x', '^']

    csvfile = open(cfg["ycsb_results_location"] + file_in, 'r')
    rows = csv.reader(csvfile, delimiter=';')

    for row in rows:
        if row[0] not in executions and row[0] != 'Execution':
            executions.append(row[0])
        if row[1] not in databases and row[1] != 'Database':
            databases.append(row[1])
        if row[2] not in threads and row[2] != 'Threads':
            threads.append(row[2])
        if row[3] not in workloads and row[3] != 'Workload':
            workloads.append(row[3])
        if row[4] not in stages and row[4] != 'Stage':
            stages.append(row[4])

        dat.append(row)
    csvfile.close()
    i = 0
    chart_data = []
    plots = []
    inserted_plots = {}
    ylim_max = 0
    chart_data_partials = []
    min_bin = np.percentile([float(m1[data_collum_number]) for m1 in dat[1:]],5)
    max_bin = np.percentile([float(m1[data_collum_number]) for m1 in dat[1:]],95)
    pl.figure(figsize=(9, 9))
    bin_size = max_bin/10
    bins = np.arange(min_bin, max_bin, bin_size)
    for db in databases:
        print db,'-op: ',operation,'th:',th,'stage:',stage
        print 'len dat: ', len(dat)
        base_list = [x1 for x1 in dat
                     if x1[1] == db
                     and x1[2] == th
                     and x1[4] == stage
                     and x1[5] == operation]
        print 'len base list: ', len(base_list)
        for d in base_list:
            for j in range(0, int(float(d[data_collum_multiplier]))):
                chart_data.append(float(d[data_collum_number]))

        print 'chart data:', len(chart_data)
        if chart_data:
            inserted_plots[str(i)] = pl.hist(chart_data, bins=bins, alpha=0.3,
                                             color=colors[i],
                                             label=db_name(db))

            plots.append(inserted_plots[str(i)])
        i = i + 1
        chart_data = []
    if len(executions) > 1:
        pl.title(titles[1])
    else:
        pl.title(titles[0])
    pl.xlabel(x_label)
    pl.ylabel(y_label)
    pl.legend(loc='lower center',
              bbox_to_anchor=(1.15,  # horizontal
                              0.02),  # vertical
              ncol=1, numpoints=1)
    # pl.xlim(0, max([float(x) for x in threads]) + 1)
    # pl.ylim(0, 20)
    # pl.xticks(tuple([float(x) for x in threads]))
    # pl.tick_params(axis='both', which='both', bottom='off', top='off',
    #                labelbottom='on', left='off', right='off',
    #                labelleft='on')
    pl.gca().spines['top'].set_visible(False)
    pl.gca().spines['right'].set_visible(False)
    pl.grid()
    # pl.tight_layout()
    if plots:
        pdf = PdfPages(file_out)
        pl.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()
    # saves the current figure into a pdf page
    pl.close()

    #             data.append(int(rows[i]))
    #
    # bins = np.linspace(math.ceil(min(data)),
    #                    math.floor(max(data)),
    #                    5)  # fixed number of bins
    #
    # plt.xlim([min(data) - 5, max(data) + 5])
    #
    # plt.hist(data, bins=bins, alpha=0.5)
    # plt.title(title)
    # plt.xlabel(xlabel)
    # plt.ylabel(ylabel)
    #
    # plt.show()

# if len(executions) == 1:
#     for db in databases:
#         for d in data:
#             if d[1] == db and d[4] == 'run':
#                 x.append(d[2])
#                 y.append(d[6])
#         p[str(i)] = pl.plot(x, y, colors[i] + 'o', label=db_name(db))
#         l[str(i)] = db
#
#         plots.append(p[str(i)])
#         legs.append(l[str(i)])
#         i = i + 1
#         x = []
#         y = []
#     pl.title('Runtime aferido ')
#     pl.xlabel('Threads')
#     pl.ylabel('Runtime (ms)')
#     pl.legend(numpoints=1)
#     pl.xlim(0, max([float(x) for x in threads]) + 5)
#     pl.xticks(tuple([float(x) for x in threads]))
#     pl.show()
# else:


def main(arg):
    # file_name = arg[0]
    # col = arg[1]
    # title = arg[2]
    # xlabel = arg[3]
    # ylabel = arg[4]
    # chart(file_name, col, title, xlabel, ylabel)
    # runtime_chart()
    histogram_charts('operations.csv', 'XXX-hist.pdf', ['', ''], '', '',
                     'run', '1', 'READ')
    line_charts('overall.csv', 'throughput-run.pdf',
                [u'Throughput aferido', u'Throughput(ops/sec) médio aferido'],
                'Threads', 'Throughput(ops/sec)', 6, 'run')
    line_charts('overall.csv', 'runtime-run.pdf',
                [u'Runtime aferido', u'Runtime médio aferido'],
                'Threads', 'Runtime (ms)', 5, 'run')
    line_charts('overall.csv', 'throughput-load.pdf',
                [u'Throughput aferido', u'Throughput(ops/sec) médio aferido'],
                'Threads', 'Throughput(ops/sec)', 6, 'load')
    line_charts('overall.csv', 'runtime-load.pdf',
                [u'Runtime aferido', u'Runtime médio aferido'],
                'Threads', 'Runtime (ms)', 5, 'load')

    op_list = operations_in_totals('totals.csv')
    for op in op_list:
        line_charts('totals.csv', op + '-num_op-run.pdf',
                    [
                        u'Número de operações aferidas',
                        u'Número médio de operações aferidas'
                    ], 'Threads',
                    'Quantidade', 6, 'run', op)
        line_charts('totals.csv', op + '-num_op-load.pdf',
                    [
                        u'Número de operações aferidas',
                        u'Número médio de operações aferidas'
                    ],
                    'Threads', 'Quantidade', 6, 'load', op)
    op_list = operations_in_totals('operations.csv')
    for op in op_list:
        for th in ['1','2','5','10']:
            histogram_charts('operations.csv', op +'-th-'+ th + '-run.pdf',
                             [op, op], 'Latency (us)', 'qtd.',
                             'run', th, op)
            histogram_charts('operations.csv', op +'-th-'+ th + '-load.pdf',
                             [op, op], 'Latency (us)', 'qtd.',
                             'load', th, op)

if __name__ == "__main__":
    main(sys.argv[1:])
