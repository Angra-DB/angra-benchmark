# -*- coding: utf-8 -*-
import csv
import json
import sys
import numpy as np
# import math
import pylab as pl
from matplotlib.backends.backend_pdf import PdfPages

cfg = None


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

def getstuff(filename, op, th):
    with open(cfg["ycsb_results_location"] + filename, "rb") as csvfile:
        datareader = csv.reader(csvfile, delimiter=';')
        for row in datareader:
            if row[5] == op and row[2] == th:
                yield row


def parameters_lists(file_in, has_operations=False):
    global cfg
    executions = []
    databases = []
    threads = []
    workloads = []
    stages = []
    operations = []
    data = []

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
        if has_operations and row[5] not in operations and row[5] != 'Operation':
            operations.append(row[5])

        data.append(row)
    csvfile.close()
    return executions, databases, threads, workloads, stages, operations, data


def line_charts(file_in, file_out, titles, x_label, y_label,
                data_collum_number, stage, operation=None):

    global cfg

    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
    markers = ['o', 's', 'D', 'x', '^']

    ret = parameters_lists(file_in, operation is not None)
    executions = ret[0]
    databases = ret[1]
    threads = ret[2]
    data = ret[6]

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
            if y_partials:
                media = sum([float(y1) for y1 in y_partials]) \
                    / len(y_partials)
            else:
                media = 0
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
    if plots:
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
        pl.xlim(0, max([float(x1) for x1 in threads]) + 1)
        pl.xticks(tuple([float(x1) for x1 in threads]))
        pl.tick_params(axis='both', which='both', bottom='off', top='off',
                       labelbottom='on', left='off', right='off',
                       labelleft='on')
        pl.gca().spines['top'].set_visible(False)
        pl.gca().spines['right'].set_visible(False)
        pl.grid()

        pdf = PdfPages(file_out)
        pl.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()
    # saves the current figure into a pdf page
    pl.close()



def histogram_charts(ret,file_in, file_out, titles, x_label, y_label,
                     stage,
                     th, operation, hist_type='nobars',
                     data_collum_number=6, data_collum_multiplier=7):

    global cfg
    databases = []
    executions = []

    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']

    # ret = parameters_lists(file_in, operation is not None)
    executions = ret[0]
    databases = ret[1]
    data = []
    count=0
    for row in getstuff(file_in, titles[0], th):
        count = count + 1
        print count
        data.append(row)

    # data =    # ret[6]

    i = 0
    ploted_dbs = []
    chart_data = []
    plots = []
    bplot_data = []
    inserted_plots = {}
    data_float = [float(m1[data_collum_number]) for m1 in data[1:]]
    min_bin = np.percentile(data_float,
                            5)
    max_bin = np.percentile(data_float,
                            95)
    pl.figure(figsize=(9, 9))
    bin_size = max_bin / 10
    bins = np.arange(min_bin, max_bin, bin_size)
    for db in databases:
        base_list = [x1 for x1 in data
                     if x1[1] == db
                     and x1[2] == th
                     and x1[4] == stage
                     and x1[5] == operation]

        for d in base_list:
            for j in range(0, int(float(d[data_collum_multiplier]))):
                chart_data.append(float(d[data_collum_number]))

        if chart_data:
            if hist_type == 'bars':
                inserted_plots[str(i)] = pl.hist(chart_data,
                                                 bins=bins,
                                                 alpha=0.3,
                                                 color=colors[i],
                                                 label=db_name(db))
            elif hist_type == 'nobars':
                hist = np.histogram(chart_data, bins=bins)
                inserted_plots[str(i)] = pl.errorbar(hist[1][:-1] + bin_size/2,
                                                     hist[0],
                                                     alpha=0.3,
                                                     xerr=bin_size/2,
                                                     capsize=0,
                                                     fmt=None,
                                                     linewidth=8,
                                                     color=colors[i],
                                                     label=db_name(db))
            elif hist_type == 'boxplot' or hist_type == 'violin':
                inserted_plots[str(i)] = '0'

                bplot_data.append(chart_data)

                # inserted_plots[str(i)]['boxes'].set_facecolor(colors[i])
                ploted_dbs.append(db)

            plots.append(inserted_plots[str(i)])
        i = i + 1
        chart_data = []

    if plots:
        if hist_type == 'boxplot':
            bplot = pl.boxplot(bplot_data,
                               notch=False,
                               vert=True,
                               patch_artist=True)
            param_zip = 'boxes'
        elif hist_type == 'violin':
            bplot = pl.violinplot(bplot_data,
                                  showmeans=False,
                                  showmedians=True,
                                  showextrema=False)
            param_zip = 'bodies'
        if hist_type == 'boxplot' or hist_type == 'violin':
            col = ['red', 'blue', 'green', 'yellow']
            for patch, color in zip(bplot[param_zip], col[0:len(plots)-1]):
                patch.set_facecolor(color)

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
        if hist_type == 'boxplot' or hist_type == 'violin':
            pl.xticks([y+1 for y in range(len(ploted_dbs))], ploted_dbs)
            # np.arange(min(x), max(x)+1, 1.0)
            minimus = 1000000000
            maximus = 0
            for cd in bplot_data:
                if min(cd) < minimus:
                    minimus = min(cd)
                if np.percentile(cd, 99) > maximus:
                    maximus = np.percentile(cd, 99)
            pl.ylim(ymax=maximus, ymin=minimus)

            # pl.yticks(np.arange(minimus, maximus, 10000))
        else:
            pl.xticks(bins + bin_size)
        # gca() = get current axes
        pl.gca().spines['top'].set_visible(False)
        pl.gca().spines['right'].set_visible(False)
        pl.grid()
        pdf = PdfPages(file_out)
        pl.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()

    pl.close()


def main(arg):

    global cfg
    cfg = load_config()

    # _, _, _, _, stages, _, _ = parameters_lists('overall.csv')
    # print 'Line - overall'
    # for st in stages:
    #     line_charts('overall.csv',
    #                 'throughput-' + st + '.pdf',
    #                 [
    #                     u'Throughput aferido',
    #                     u'Throughput(ops/sec) médio aferido'
    #                 ],
    #                 'Threads',
    #                 'Throughput(ops/sec)',
    #                 6,
    #                 st)
    #     line_charts('overall.csv',
    #                 'runtime-' + st + '.pdf',
    #                 [
    #                     u'Runtime aferido',
    #                     u'Runtime médio aferido'
    #                 ],
    #                 'Threads',
    #                 'Runtime (ms)',
    #                 5,
    #                 st)
    #
    # _, _, _, _, stages, op_list, _ = parameters_lists('totals.csv', True)
    # print 'Line - totals'
    # for op in op_list:
    #     for st in stages:
    #         line_charts('totals.csv',
    #                     'line-' + op + '-num_op-' + st + '.pdf',
    #                     [
    #                         u'Número de operações aferidas',
    #                         u'Número médio de operações aferidas'
    #                     ],
    #                     'Threads',
    #                     'Quantidade',
    #                     6,
    #                     st,
    #                     op)

    #_, _, threads, _, stages, op_list, _ = parameters_lists('operations.csv', True)
    ret = parameters_lists('totals.csv', True)
    # _, _, threads, _, stages, op_list, _ = parameters_lists('totals.csv', True)
    op_list = ret[5]
    threads = ret[2]
    stages = ret[4]
    print 'Histogram - operations'
    for op in op_list:
        for th in threads:
            for st in stages:
                # histogram_charts('operations.csv',
                #                  'hist-' + op + '-th-' + th + '-' + st + '.pdf',
                #                  [
                #                      op,
                #                      op
                #                  ],
                #                  'Latency (us)',
                #                  'qtd.',
                #                  st,
                #                  th,
                #                  op)
                histogram_charts(ret, 'operations.csv',
                                 'bplot-' + op + '-th-' + th + '-' + st + '.pdf',
                                 [
                                     op,
                                     op
                                 ],
                                 'SGBD',
                                 'Latency (us)',
                                 st,
                                 th,
                                 op, 'violin')


if __name__ == "__main__":
    main(sys.argv[1:])
