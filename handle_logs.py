# -*- coding: utf-8 -*-
import csv
import glob
import json
import sys


cfg = None
server_os_user = ''


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def load_config():
    data = []
    for config_file in glob.glob('*.json'):
        with open(config_file, "rb") as cf:
            data.append(json.load(cf))
    dic = {}
    for item in data:
        dic.update(item)
    return dic


def time_stamp_file():
    return time_stamp().replace('-', '').replace(':', '').replace(' ', '')


def time_stamp():
    import datetime
    import time
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def log_types(type, ex, com_type, db, th, wl):
    global cfg

    if type == 'result':
        to_return = cfg["ycsb_results_location"] + str(ex) + '-' + \
            com_type + '-' + db + '-threads-' + str(th) + '-' +\
            wl + '.txt'
    elif type == 'screen':
        to_return = time_stamp() + ' - Running ' + com_type + ' number ' + \
            str(ex) + ' for ' + db + ' with ' + str(th) + \
            ' threads of workload ' + wl

    return to_return


def read_result_files():
    global cfg
    overall_list = []
    gc_totals_list = []
    unknow_list = []
    operations_list = []
    totals_list = []

    create_first_line(
        operations_list,
        totals_list,
        overall_list,
        gc_totals_list,
        unknow_list
    )

    create_cvs_files(
        'csv',
        [
            unknow_list, overall_list, gc_totals_list,
            operations_list, totals_list
        ],
        [
            'unknow', 'overall', 'GC_totals',
            'operations', 'totals'
        ],
        cfg["ycsb_results_location"]
    )

    overall_list = []
    gc_totals_list = []
    unknow_list = []
    operations_list = []
    totals_list = []

    print time_stamp(), 'reading files (start)'
    for file_type in cfg["stages"]:
        for db in cfg["dbs"]:
            for th in cfg["threads"]:
                for ex in range(1, cfg["executions"] + 1):
                    for wl in cfg["workloads"]:
                        file_name = log_types('result', ex, file_type, db,
                                              th, wl)
                        with open(file_name, 'r') as csvfile:
                            result_file = csv.reader(csvfile, delimiter=',')
                            for row in result_file:
                                read_line_from_result(
                                    db, str(ex), str(th),
                                    wl, file_type, row,
                                    operations_list,
                                    totals_list,
                                    overall_list,
                                    gc_totals_list,
                                    unknow_list
                                )
                            csvfile.close()
                            unknow_list = clean_unkow(unknow_list)
                            print time_stamp(), 'reading ', file_name
                            export_cvs_files(
                                'csv',
                                [
                                    unknow_list, overall_list,
                                    gc_totals_list,
                                    operations_list, totals_list
                                ],
                                [
                                    'unknow', 'overall',
                                    'GC_totals',
                                    'operations', 'totals'
                                ],
                                cfg["ycsb_results_location"]
                            )
                            overall_list = []
                            gc_totals_list = []
                            unknow_list = []
                            operations_list = []
                            totals_list = []
    print time_stamp(), 'reading files (end)'

    # unknow_list = clean_unkow(unknow_list)

    # export_cvs_files('csv',
    #                  [
    #                      unknow_list, overall_list, gc_totals_list,
    #                      operations_list, totals_list
    #                  ],
    #                  [
    #                      'unknow', 'overall', 'GC_totals',
    #                      'operations', 'totals'
    #                  ],
    #                  cfg["ycsb_results_location"])


def create_first_line(lst_ops, lst_totals,
                      lst_overall, lst_t_gc, lst_unknow):

    lst_totals.append(
        ['Execution', 'Database', 'Threads',
         'Workload', 'Stage', 'Operation',
         '# Operations', 'AverageLatency(us)',
         'MinLatency(us)', 'MaxLatency(us)',
         '95thPercentileLatency(us)', '99thPercentileLatency(us)',
         'Return=OK', 'Return=NOT_FOUND',
         'Return=UNEXPECTED_STATE', 'Return=ERROR']
    )

    lst_ops.append(['Execution',
                    'Database',
                    'Threads',
                    'Workload',
                    'Stage',
                    'Operation',
                    'Latency (us)', 'Frequence'])

    lst_t_gc.append(
        [
            'Execution', 'Database', 'Threads', 'Workload', 'Stage',
            'GCS Copy [Count]',
            'GCS G1 Young Generation [Count]',
            'GCS G1 Old Generation [Count]',
            'GC Time Copy [Time(ms)]',
            'GC Time % Copy [Time(%)]',
            'GC Time G1 Young Generation [Time(ms)]',
            'GC Time % G1 Young Generation [Time(%)]',
            'GC Time G1 Old Generation [Time(ms)]',
            'GC Time % G1 Old Generation [Time(%)]',
            'GCS MarkSweepCompact [Count]',
            'GC Time MarkSweepCompact [Time(ms)]',
            'GC Time % MarkSweepCompact [Time(%)]',
            'GCs [Count]',
            'GC Time [Time(ms)]',
            'GC Time % [Time(%)]',
            'GCS PS MarkSweep [Count]',
            'GC PS Time MarkSweep [Time(ms)]',
            'GC Time % PS MarkSweep [Time(%)]',
            'GCS PS Scavenge [Count]',
            'GC Time PS Scavenge [Time(ms)]',
            'GC Time % PS Scavenge [Time(%)]'
        ]
    )

    lst_overall.append(['Execution', 'Database', 'Threads',
                        'Workload', 'Stage',
                        'RunTime(ms)',
                        'Throughput(ops/sec)'])


def read_line_from_result(db, ex, th, wl, stage, row, lst_ops, lst_totals,
                          lst_overall, lst_t_gc, lst_unknow):

    empty_col = ''
    if row[0] in ('[READ]', '[READ-FAILED]', '[UPDATE]', '[UPDATE-FAILED]',
                  '[INSERT]', '[CLEANUP]', '[INSERT-FAILED]'):
        hadle_operation_lines(
            db, ex, th, wl, stage, row, lst_unknow, empty_col,
            lst_totals, lst_ops
        )
    elif '[TOTAL_GC' in row[0]:
        handle_GC_lines(
            db, ex, th, wl, stage, row, lst_unknow, empty_col,
            lst_t_gc,
        )
    elif row[0] == '[OVERALL]':
        handle_overall_lines(
            db, ex, th, wl, stage, row, lst_unknow, empty_col,
            lst_overall
        )
    else:
        lst_unknow.append(''.join(row))


def hadle_operation_lines(db, ex, th, wl, stage, row, lst_unknow, empty_col,
                          lst_totals, lst_ops):
    if str(row[1].strip()) in ['Operations',
                               'AverageLatency(us)',
                               'MinLatency(us)',
                               'MaxLatency(us)',
                               '95thPercentileLatency(us)',
                               '99thPercentileLatency(us)',
                               'Return=OK',
                               'Return=NOT_FOUND',
                               'Return=UNEXPECTED_STATE',
                               'Return=ERROR']:

        if not [item for item in lst_totals if
                item[0] == ex
                and item[1] == db
                and item[2] == th
                and item[3] == wl
                and item[4] == stage
                and item[5] == row[0].replace('[', '').replace(']', '')]:
            lst_totals.append(
                [ex, db, th, wl, stage,
                 row[0].replace('[', '').replace(']', ''),
                 empty_col, empty_col, empty_col,
                 empty_col, empty_col, empty_col,
                 empty_col, empty_col, empty_col,
                 empty_col]
            )

        if str(row[1].strip()) == 'Operations':
            lst_totals[-1][6] = str(row[2].strip())
        elif str(row[1].strip()) == 'AverageLatency(us)':
            lst_totals[-1][7] = str(row[2].strip())
        elif str(row[1].strip()) == 'MinLatency(us)':
            lst_totals[-1][8] = str(row[2].strip())
        elif str(row[1].strip()) == 'MaxLatency(us)':
            lst_totals[-1][9] = str(row[2].strip())
        elif str(row[1].strip()) == '95thPercentileLatency(us)':
            lst_totals[-1][10] = str(row[2].strip())
        elif str(row[1].strip()) == '99thPercentileLatency(us)':
            lst_totals[-1][11] = str(row[2].strip())
        elif str(row[1].strip()) == 'Return=OK':
            lst_totals[-1][12] = str(row[2].strip())
        elif str(row[1].strip()) == 'Return=NOT_FOUND':
            lst_totals[-1][13] = str(row[2].strip())
        elif str(row[1].strip()) == 'Return=UNEXPECTED_STATE':
            lst_totals[-1][14] = str(row[2].strip())
        elif str(row[1].strip()) == 'Return=ERROR':
            lst_totals[-1][15] = str(row[2].strip())

    elif (is_number(row[1].strip()) and is_number(row[2].strip())):
        lst_ops.append([ex, db, th, wl, stage,
                        row[0].replace('[', '').replace(']', ''),
                        'error', 'error'])

        lst_ops[-1][6] = str(row[1].strip())
        lst_ops[-1][7] = str(row[2].strip())
    else:
        lst_unknow.append(''.join(row))


def handle_GC_lines(db, ex, th, wl, stage, row, lst_unknow, empty_col,
                    lst_t_gc):
    if not [item for item in lst_t_gc
            if item[0] == ex
            and item[1] == db
            and item[2] == th
            and item[3] == wl
            and item[4] == stage]:
        lst_t_gc.append([ex, db, th, wl, stage,
                         empty_col, empty_col, empty_col, empty_col,
                         empty_col, empty_col, empty_col, empty_col,
                         empty_col, empty_col, empty_col, empty_col,
                         empty_col, empty_col, empty_col, empty_col,
                         empty_col, empty_col, empty_col, empty_col,
                         empty_col])

    if str(row[0].strip()) == '[TOTAL_GCS_Copy]':
        lst_t_gc[-1][5] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GCS_G1_Young_Generation]':
        lst_t_gc[-1][6] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GCS_G1_Old_Generation]':
        lst_t_gc[-1][7] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_Copy]':
        lst_t_gc[-1][8] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_Copy]':
        lst_t_gc[-1][9] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_G1_Young_Generation]':
        lst_t_gc[-1][10] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_G1_Young_Generation]':
        lst_t_gc[-1][11] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_G1_Old_Generation]':
        lst_t_gc[-1][12] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_G1_Old_Generation]':
        lst_t_gc[-1][13] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GCS_MarkSweepCompact]':
        lst_t_gc[-1][14] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_MarkSweepCompact]':
        lst_t_gc[-1][15] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_MarkSweepCompact]':
        lst_t_gc[-1][16] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GCs]':
        lst_t_gc[-1][17] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME]':
        lst_t_gc[-1][18] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_%]':
        lst_t_gc[-1][19] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GCS_PS_MarkSweep]':
        lst_t_gc[-1][20] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_PS_MarkSweep]':
        lst_t_gc[-1][21] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_PS_MarkSweep]':
        lst_t_gc[-1][22] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GCS_PS_Scavenge]':
        lst_t_gc[-1][23] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_PS_Scavenge]':
        lst_t_gc[-1][24] = str(row[2].strip())
    elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_PS_Scavenge]':
        lst_t_gc[-1][25] = str(row[2].strip())

    else:
        lst_unknow.append(''.join(row))


def handle_overall_lines(db, ex, th, wl, stage, row, lst_unknow, empty_col,
                         lst_overall):
    if not [item for item in lst_overall
            if item[0] == ex
            and item[1] == db
            and item[2] == th
            and item[3] == wl
            and item[4] == stage]:
        lst_overall.append([ex, db, th, wl, stage,
                            empty_col, empty_col])

    if str(row[1].strip()) == 'RunTime(ms)':
        lst_overall[-1][5] = str(row[2].strip())
    elif str(row[1].strip()) == 'Throughput(ops/sec)':
        lst_overall[-1][6] = str(row[2].strip())
    else:
        lst_unknow.append(''.join(row))


def clean_unkow(lst_unknow):
    know_unknow = [
        'Adding shard node URL:',
        'Using shards:',
        'Adding shard node URL:',
        'mongo client connection created with',
        'ycsb_home:::'
    ]
    for k in know_unknow:
        lst_unknow = [x for x in lst_unknow if k not in x]
    return lst_unknow


def create_cvs_files(sufix, lists,
                     file_name_list, ycsb_results_location):
    # print time_stamp(), 'Creating', sufix, 'files (start)'
    for i in range(0, len(lists)):
            new_file = open(ycsb_results_location + file_name_list[i] +
                            '.' + sufix, 'w')
            for row in lists[i]:
                if i == 0:  # uknow list
                    new_file.write(''.join(row) + '\n')
                else:  # another lists
                    new_file.write(';'.join(row) + '\n')
            new_file.close()


def export_cvs_files(sufix, lists,
                     file_name_list, ycsb_results_location):
    # print time_stamp(), 'Creating', sufix, 'files (start)'
    for i in range(0, len(lists)):
        if len(lists[i]) >= 1:
            new_file = open(ycsb_results_location + file_name_list[i] +
                            '.' + sufix, 'a')
            for row in lists[i]:
                if i == 0:  # uknow list
                    new_file.write(''.join(row) + '\n')
                else:  # another lists
                    new_file.write(';'.join(row) + '\n')
            new_file.close()

    # print time_stamp(), 'Creating', sufix, 'files (start)'


def main(arg):
    global cfg
    cfg = load_config()

    if arg[0] == 'csv':
        make_csv = True

    elif arg[0] == 'cfg':
        for key, value in cfg.items():
            print key, ':', value
        make_csv = False

    if make_csv:
        read_result_files()


if __name__ == "__main__":
    main(sys.argv[1:])
