#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module create charts of a result set of YCSB.

Can create line, boxplots, violinplot and histograms charts. Use JSON file as
configuration file.
"""

import csv
import glob
import json
import sys
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import pylab as pl

CFG = None


def time_stamp():
    """Return time stamp for log."""
    import datetime
    import time
    t_s = time.time()
    return datetime.datetime.fromtimestamp(t_s).strftime('%Y-%m-%d %H:%M:%S')


def operations_in_totals(file_in):
    """
    Return a list of operations in total YCSB result file.

    Args:
        file_in (file): YCSB result file.
    Returns:
        list: Operations types in file_in.

    """
    global CFG
    csvfile = open(CFG["ycsb_results_location"] + file_in, 'r')
    rows = csv.reader(csvfile, delimiter=';')
    operations_list = []
    for row in rows:
        if row[5] not in operations_list and row[5] != 'Operation':
            operations_list.append(row[5])
    return operations_list


def db_name(name):
    """Convert given file DB names to use in charts labels."""
    if name == "mongodb":
        return "MongoDB"
    if name == "angra":
        return "Angra-DB"
    if name == "mysql":
        return "MySQL"
    if name == "couchdb":
        return "CouchDB"
    return name


def load_config():
    """Load all JSON files in script path to use as script parameters."""
    data = []
    for config_file in glob.glob('*.json'):
        with open(config_file, "rb") as c_f:
            data.append(json.load(c_f))
    dic = {}
    for item in data:
        dic.update(item)
    return dic


def filter_lines(filename, operation, thread, stage):
    """
    Generate lisnes for filtered_rows function.

    Args:
        filename (string): YCSB result file.
        operation (string): Test operation type.
        therads (int): number of test threads.
        stage (string): test stage (load or run).

    Yields:
        list: rows with given parameters.

    """
    with open(CFG["ycsb_results_location"] + filename, "rb") as csvfile:
        rows = csv.reader(csvfile, delimiter=';')
        for row in rows:
            if (
                    (operation is None or row[5] == operation)
                    and (thread is None or row[2] == thread)
                    and (stage is None or row[4] == stage)):
                yield row


def filtered_rows(filename, operation, thread, stage):
    """
    Select rows with given parameters of YCSB result file.

    Args:
        filename (string): YCSB result file.
        operation (string): Test operation type.
        therads (int): number of test threads.
        stage (string): test stage (load or run).

    Return:
        list: rows with given parameters.

    """
    data = []
    for row in filter_lines(filename, operation, thread, stage):
        data.append(row)
    return data


def line_charts(data, file_out, titles, x_label, y_label,
                data_collum_number, stage, operation=None):
    """
    Create and save PNG and PDF files with a line chart of given parameters.

    Args:
        data (list): Data to be analised.
        file_out (string): Name of generated files,
        title (string): Title to be writen above the chart.
        x_label (string): Label of X axe.
        y_label (string): Label of Y axe.
        data_collum_number (int): Number of collum to be analised on data list.
        stage (string): stage of test to be analised.
        operation (string): Whitch operation to analise.
    """
    global CFG

    i = 0
    x_axe = []
    y_axe = []
    plots = []
    inserted_plots = {}
    ylim_max = 0
    y_partials = []

    pl.figure(figsize=(9, 9))
    for dba in CFG["dbs"]:
        for thr in CFG["threads"]:
            x_axe.append(thr)
            for ex in range(1, CFG["executions"]):
                if operation is None:
                    base_list = [x1 for x1 in data
                                 if x1[0] == str(ex)
                                 and x1[1] == dba
                                 and x1[2] == str(thr)
                                 and x1[4] == stage]
                else:
                    base_list = [x1 for x1 in data
                                 if x1[0] == str(ex)
                                 and x1[1] == dba
                                 and x1[2] == str(thr)
                                 and x1[4] == stage
                                 and x1[5] == operation]

                for dat in base_list:
                    y_partials.append(dat[data_collum_number])
                    if float(dat[data_collum_number]) > ylim_max:
                        ylim_max = float(dat[data_collum_number])
            if y_partials:
                media = sum([float(y1) for y1 in y_partials]) \
                    / len(y_partials)
            else:
                media = 0
            y_axe.append(media)
            y_partials = []
        if [y2 for y2 in y_axe if y2 != 0]:
            inserted_plots[str(i)] = pl.plot(
                x_axe, y_axe,
                CFG["colors"][i] + '-' + CFG["markers"][i],
                label=db_name(dba))

            plots.append(inserted_plots[str(i)])
        i = i + 1
        x_axe = []
        y_axe = []
    if plots:
        if CFG["executions"] > 1:
            pl.title(titles[1])
        else:
            pl.title(titles[0])
        pl.xlabel(x_label)
        pl.ylabel(y_label)
        pl.legend(loc='lower center',
                  bbox_to_anchor=(1.15,  # horizontal
                                  0.02),  # vertical
                  ncol=1, numpoints=1)
        pl.xlim(0, max([float(x1) for x1 in CFG["threads"]]) + 1)
        pl.xticks(tuple([float(x1) for x1 in CFG["threads"]]))
        pl.tick_params(axis='both', which='both', bottom='off', top='off',
                       labelbottom='on', left='off', right='off',
                       labelleft='on')
        pl.gca().spines['top'].set_visible(False)
        pl.gca().spines['right'].set_visible(False)
        pl.grid()

        # pdf = PdfPages(CFG["charts_location"] + file_out)
        # pl.savefig(pdf, format='pdf', bbox_inches='tight')
        # pdf.close()

        fig = pl.gcf()
        pdf = PdfPages(CFG["charts_location"] + file_out + '.pdf')

        fig.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()
        fig.savefig(
            CFG["charts_location"] + file_out + '.png',
            dpi=300,
            format='png',
            bbox_inches='tight')

    # saves the current figure into a pdf page
    pl.close()


def histogram_charts(data, file_out, titles,
                     x_label, y_label, stage,
                     thread, operation, hist_type='nobars',
                     data_collum_number=6, data_collum_multiplier=7):
    """
    Create and save PNG and PDF files with a line chart of given parameters.

    Args:
        data (list): Data to be analised.
        file_out (string): Name of generated files,
        titles (string): Title to be writen above the chart.
        x_label (string): Label of X axe.
        y_label (string): Label of Y axe.
        stage (string): stage of test to be analised.
        thread (int): Number of threads of test to be analised.
        operation (string): Whitch operation to analise.
        hist_type (string): Chart type to be generated, can be `violin`,
`boxplot` or 'nobar'.
        data_collum_number (int): Number of collum to be analised on data list.
        data_collum_multiplier (int): ?

    """
    global CFG

    # ret = parameters_lists(file_in, operation is not None)
    executions = CFG["executions"]  # ret[0]
    databases = CFG["dbs"]  # ret[1]

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
    for dba in databases:
        base_list = [x1 for x1 in data
                     if x1[1] == dba
                     and x1[2] == str(thread)
                     and x1[4] == stage
                     and x1[5] == operation]

        for dat in base_list:
            for _ in range(0, int(float(dat[data_collum_multiplier]))):
                chart_data.append(float(dat[data_collum_number]))

        if chart_data:
            if hist_type == 'bars':
                inserted_plots[str(i)] = pl.hist(chart_data,
                                                 bins=bins,
                                                 alpha=0.3,
                                                 color=CFG["colors"][i],
                                                 label=db_name(dba))
            elif hist_type == 'nobars':
                hist = np.histogram(chart_data, bins=bins)
                inserted_plots[str(i)] = pl.errorbar(hist[1][:-1] + bin_size/2,
                                                     hist[0],
                                                     alpha=0.3,
                                                     xerr=bin_size/2,
                                                     capsize=0,
                                                     fmt="none",
                                                     linewidth=8,
                                                     color=CFG["colors"],
                                                     label=db_name(dba))
            elif hist_type == 'boxplot' or hist_type == 'violin':
                inserted_plots[str(i)] = '0'
                bplot_data.append(chart_data)
                ploted_dbs.append(dba)
            plots.append(inserted_plots[str(i)])
        i = i + 1
        chart_data = []

    if plots:
        pl.ticklabel_format(style='plain', axis='both')
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
            minimus = 1000000000
            maximus = 0
            for cda in bplot_data:
                if min(cda) < minimus:
                    minimus = min(cda)
                if np.percentile(cda, 99) > maximus:
                    maximus = np.percentile(cda, 99)
            pl.ylim(ymax=maximus, ymin=minimus)

            # pl.yticks(np.arange(minimus, maximus, 10000))
        else:
            pl.xticks(bins + bin_size)
        # gca() = get current axes
        pl.gca().spines['top'].set_visible(False)
        pl.gca().spines['right'].set_visible(False)
        pl.grid()
        fig = pl.gcf()
        pdf = PdfPages(CFG["charts_location"] + file_out + '.pdf')

        fig.savefig(pdf, format='pdf', bbox_inches='tight')
        pdf.close()
        fig.savefig(
            CFG["charts_location"] + file_out + '.png',
            dpi=300,
            format='png',
            bbox_inches='tight')

    pl.close()


def main(arg):
    """Create charts with given configuration."""
    global CFG
    CFG = load_config()

    if arg == "config":
        print CFG
        return

    if [
            i for i in CFG["chart_types"]
            if i in ["line-throughput", "line-runtime"]]:
        print time_stamp(), 'creating overall Line charts'
        for sta in CFG["stages"]:
            print 'Executing for stage', sta
            print time_stamp(), '- getting data - start'
            data = filtered_rows('overall.csv', None, None, sta)
            print time_stamp(), '- getting data - end'
            if not data:
                print 'no data'
            else:
                if "line-throughput" in CFG["chart_types"]:
                    line_charts(
                        data,
                        'throughput-' + sta,
                        [
                            u'Throughput aferido',
                            u'Throughput(ops/sec) médio aferido'
                            ],
                        'Threads',
                        'Throughput(ops/sec)',
                        6,
                        sta
                        )
                if "line-runtime" in CFG["chart_types"]:
                    line_charts(
                        data,
                        'runtime-' + sta,
                        [
                            u'Runtime aferido',
                            u'Runtime médio aferido'
                            ],
                        'Threads',
                        'Runtime (ms)',
                        5,
                        sta
                        )
    if "line-operations" in CFG["chart_types"]:
        print time_stamp(), 'creating totals per operation Line charts'
        for ope in CFG["operations"]:
            for sta in CFG["stages"]:
                print 'Executing for', ope, 'in', sta, 'stage'
                print time_stamp(), '- getting data - start'
                data = filtered_rows('totals.csv', ope, None, sta)
                print time_stamp(), '- getting data - end'
                if not data:
                    print 'no data'
                else:
                    line_charts(
                        data,
                        'line-' + ope + '-num_op-' + sta,
                        [
                            u'Número de operações aferidas',
                            u'Número médio de operações aferidas'
                            ],
                        'Threads',
                        'Quantidade',
                        6,
                        sta,
                        ope
                        )

    if [
            i for i in CFG["chart_types"]
            if i in ["histogram", "boxplot", "violin"]]:
        print time_stamp(), 'creating operation latency charts'
        for ope in CFG["operations"]:
            for thr in CFG["threads"]:
                for sta in CFG["stages"]:
                    print 'Executing for', ope, 'over', thr,
                    print 'threads in', sta, 'stage'
                    print time_stamp(), '- getting data - start'
                    data = filtered_rows('operations.csv', ope, str(thr), sta)
                    print time_stamp(), '- getting data - end'
                    if not data:
                        print 'no data'
                    else:
                        if "histogram" in CFG["chart_types"]:
                            histogram_charts(
                                data,
                                'hist-' + ope + '-th-' + str(thr) + '-' + sta,
                                [
                                    ope,
                                    ope
                                    ],
                                'Latency (us)',
                                'qtd.',
                                sta,
                                thr,
                                ope
                                )
                        if "violin" in CFG["chart_types"]:
                            histogram_charts(
                                data,
                                'violin-' + ope + '-th-' + str(thr) + '-'
                                + sta,
                                [
                                    ope,
                                    ope
                                    ],
                                'SGBD',
                                'Latency (us)',
                                sta,
                                thr,
                                ope, 'violin')
                        if "boxplot" in CFG["chart_types"]:
                            histogram_charts(
                                data,
                                'boxplot-' + ope + '-th-' + str(thr) + '-'
                                + sta,
                                [
                                    ope,
                                    ope
                                    ],
                                'SGBD',
                                'Latency (us)',
                                sta,
                                thr,
                                ope, 'boxplot')


if __name__ == "__main__":
    main(sys.argv[1:])
