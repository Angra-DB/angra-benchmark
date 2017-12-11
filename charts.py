# -*- coding: utf-8 -*-

import csv
import glob
import json
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import pylab as pl
import sys
cfg = None


def time_stamp():
    import datetime
    import time
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def operations_in_totals(file_in):
    global cfg
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
    data = []
    for config_file in glob.glob('*.json'):
        with open(config_file, "rb") as cf:
            data.append(json.load(cf))
    dic = {}
    for item in data:
        dic.update(item)
    return dic


def filter_lines(filename, operation, thread, stage):
    with open(cfg["ycsb_results_location"] + filename, "rb") as csvfile:
        rows = csv.reader(csvfile, delimiter=';')
        for r in rows:
            if (
                    (operation is None or r[5] == operation) and
                    (thread is None or r[2] == thread) and
                    (stage is None or r[4] == stage)
            ):
                yield r


def filtered_rows(filename, operation, thread, stage):
    data = []
    for row in filter_lines(filename, operation, thread, stage):
        data.append(row)
    return data
# def parameters_lists(file_in, has_operations=False):
#     global cfg
#     executions = []
#     databases = []
#     threads = []
#     workloads = []
#     stages = []
#     operations = []
#     data = []
#
#     csvfile = open(cfg["ycsb_results_location"] + file_in, 'r')
#     rows = csv.reader(csvfile, delimiter=';')
#
#     for row in rows:
#         if row[0] not in executions and row[0] != 'Execution':
#             executions.append(row[0])
#         if row[1] not in databases and row[1] != 'Database':
#             databases.append(row[1])
#         if row[2] not in threads and row[2] != 'Threads':
#             threads.append(row[2])
#         if row[3] not in workloads and row[3] != 'Workload':
#             workloads.append(row[3])
#         if row[4] not in stages and row[4] != 'Stage':
#             stages.append(row[4])
#         if has_operations and row[5] not in operations and row[5] != 'Operation':
#             operations.append(row[5])
#
#         data.append(row)
#     csvfile.close()
#     return executions, databases, threads, workloads, stages, operations, data


def line_charts(data, file_out, titles, x_label, y_label,
                data_collum_number, stage, operation=None):
    print 'entrou line'
    global cfg

    # ret = parameters_lists(file_in, operation is not None)

    # data = filtered_rows(file_in, operation, None, stage)
    # data = ret[6]

    i = 0
    x = []
    y = []
    plots = []
    inserted_plots = {}
    ylim_max = 0
    y_partials = []

    pl.figure(figsize=(9, 9))
    for db in cfg["dbs"]:
        for th in cfg["threads"]:
            x.append(th)
            for ex in range(1, cfg["executions"]):
                if operation is None:
                    base_list = [x1 for x1 in data
                                 if x1[0] == str(ex)
                                 and x1[1] == db
                                 and x1[2] == str(th)
                                 and x1[4] == stage]
                else:
                    base_list = [x1 for x1 in data
                                 if x1[0] == str(ex)
                                 and x1[1] == db
                                 and x1[2] == str(th)
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
            inserted_plots[str(i)] = pl.plot(
                x, y,
                cfg["colors"][i] + '-' + cfg["markers"][i],
                label=db_name(db))

            plots.append(inserted_plots[str(i)])
        i = i + 1
        x = []
        y = []
    if plots:
        if cfg["executions"] > 1:
            pl.title(titles[1])
        else:
            pl.title(titles[0])
        pl.xlabel(x_label)
        pl.ylabel(y_label)
        pl.legend(loc='lower center',
                  bbox_to_anchor=(1.15,  # horizontal
                                  0.02),  # vertical
                  ncol=1, numpoints=1)
        pl.xlim(0, max([float(x1) for x1 in cfg["threads"]]) + 1)
        pl.xticks(tuple([float(x1) for x1 in cfg["threads"]]))
        pl.tick_params(axis='both', which='both', bottom='off', top='off',
                       labelbottom='on', left='off', right='off',
                       labelleft='on')
        pl.gca().spines['top'].set_visible(False)
        pl.gca().spines['right'].set_visible(False)
        pl.grid()

        # pdf = PdfPages(cfg["charts_location"] + file_out)
        # pl.savefig(pdf, format='pdf', bbox_inches='tight')
        # pdf.close()

        fig = pl.gcf()
        pdf = PdfPages(cfg["charts_location"] + file_out + '.pdf')

        fig.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()
        fig.savefig(
            cfg["charts_location"] + file_out + '.png',
            dpi=300,
            format='png',
            bbox_inches='tight')

    # saves the current figure into a pdf page
    pl.close()


def histogram_charts(data, file_out, titles,
                     x_label, y_label, stage,
                     thread, operation, hist_type='nobars',
                     data_collum_number=6, data_collum_multiplier=7):

    global cfg

    # ret = parameters_lists(file_in, operation is not None)
    executions = cfg["executions"]  # ret[0]
    databases = cfg["dbs"]  # ret[1]


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
                     and x1[2] == str(thread)
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
                                                 color=cfg["colors"][i],
                                                 label=db_name(db))
            elif hist_type == 'nobars':
                hist = np.histogram(chart_data, bins=bins)
                inserted_plots[str(i)] = pl.errorbar(hist[1][:-1] + bin_size/2,
                                                     hist[0],
                                                     alpha=0.3,
                                                     xerr=bin_size/2,
                                                     capsize=0,
                                                     fmt="none",
                                                     linewidth=8,
                                                     color=cfg["colors"],
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
        pl.ticklabel_format(style='plain', axis='both')
        if hist_type == 'boxplot':
            bplot = pl.boxplot(bplot_data,
                               notch=False,
                               vert=True,
                               patch_artist=True,
                               label='_none')
            param_zip = 'boxes'
        elif hist_type == 'violin':
            bplot = pl.violinplot(bplot_data,
                                  showmeans=False,
                                  showmedians=True,
                                  showextrema=False,
                                  label='_none')
            param_zip = 'bodies'
        if hist_type == 'boxplot' or hist_type == 'violin':
            col = ['red', 'blue', 'green', 'yellow']
            for patch, color in zip(bplot[param_zip], col[0:len(plots)-1]):
                patch.set_facecolor(color)

        if executions > 1:
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
        fig = pl.gcf()
        pdf = PdfPages(cfg["charts_location"] + file_out + '.pdf')

        fig.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()
        fig.savefig(
            cfg["charts_location"] + file_out + '.png',
            dpi=300,
            format='png',
            bbox_inches='tight')

    pl.close()


def main(arg):
    global cfg
    cfg = load_config()
    if [
            i for i in cfg["chart_types"]
            if i in ["line-throughput", "line-runtime"]
    ]:
        print time_stamp(), 'creating overall Line charts'
        for st in cfg["stages"]:
            print 'Executing for stage', st
            print time_stamp(), '- getting data - start'
            data = filtered_rows('overall.csv', None, None, st)
            print time_stamp(), '- getting data - end'
            if not data:
                print 'no data'
            else:
                if "line-throughput" in cfg["chart_types"]:
                    line_charts(
                        data,
                        'throughput-' + st,
                        [
                            u'Throughput aferido',
                            u'Throughput(ops/sec) médio aferido'
                        ],
                        'Threads',
                        'Throughput(ops/sec)',
                        6,
                        st
                    )
                if "line-runtime" in cfg["chart_types"]:
                    line_charts(
                        data,
                        'runtime-' + st,
                        [
                            u'Runtime aferido',
                            u'Runtime médio aferido'
                        ],
                        'Threads',
                        'Runtime (ms)',
                        5,
                        st
                    )
    if "line-operations" in cfg["chart_types"]:
        print time_stamp(), 'creating totals per operation Line charts'
        for op in cfg["operations"]:
            for st in cfg["stages"]:
                print 'Executing for', op, 'in', st, 'stage'
                print time_stamp(), '- getting data - start'
                data = filtered_rows('totals.csv', op, None, st)
                print time_stamp(), '- getting data - end'
                if not data:
                    print 'no data'
                else:
                    line_charts(
                        data,
                        'line-' + op + '-num_op-' + st,
                        [
                            u'Número de operações aferidas',
                            u'Número médio de operações aferidas'
                        ],
                        'Threads',
                        'Quantidade',
                        6,
                        st,
                        op
                    )

    if [
            i for i in cfg["chart_types"]
            if i in ["histogram", "boxplot", "violin"]
    ]:
        print time_stamp(), 'creating operation latency charts'
        for op in cfg["operations"]:
            for th in cfg["threads"]:
                for st in cfg["stages"]:
                    print 'Executing for', op, 'over', th,
                    print 'threads in', st, 'stage'
                    print time_stamp(), '- getting data - start'
                    data = filtered_rows('operations.csv', op, str(th), st)
                    print time_stamp(), '- getting data - end'
                    if not data:
                        print 'no data'
                    else:
                        if "histogram" in cfg["chart_types"]:
                            histogram_charts(
                                data,
                                'hist-' + op + '-th-' + str(th) + '-' + st,
                                [
                                    op,
                                    op
                                ],
                                'Latency (us)',
                                'qtd.',
                                st,
                                th,
                                op
                            )
                        if "violin" in cfg["chart_types"]:
                            histogram_charts(
                                data,
                                'violin-' + op + '-th-' + str(th) + '-' + st,
                                [
                                    op,
                                    op
                                ],
                                'SGBD',
                                'Latency (us)',
                                st,
                                th,
                                op, 'violin')
                        if "boxplot" in cfg["chart_types"]:
                            histogram_charts(
                                data,
                                'boxplot-' + op + '-th-' + str(th) + '-' + st,
                                [
                                    op,
                                    op
                                ],
                                'SGBD',
                                'Latency (us)',
                                st,
                                th,
                                op, 'boxplot')


if __name__ == "__main__":
    main(sys.argv[1:])
