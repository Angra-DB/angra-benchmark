from subprocess import Popen, PIPE, check_output
from time import sleep
import csv
import sys
import os
import json
import subprocess

ON_POSIX = 'posix' in sys.builtin_module_names

cfg = None
server_os_user = ''


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def load_config():
    with open('benchmark.cfg') as data_file:
        data = json.load(data_file)
    return data


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


def init_used_databases(log_file):
    global cfg
    for db in cfg["dbs"]:
        if db == 'angra':
            db_process = start_angra(log_file)
            kill_angra(db_process, log_file)
            remove_angra_files(log_file)
        elif db == 'mongodb':
            db_process = start_mongodb(log_file)
            remove_mongodb_files(log_file)
            kill_mongodb(db_process, log_file)
        elif db == 'mysql':
            db_process = start_mysql(log_file)
            remove_mysql_files(log_file)
            kill_mysql(db_process, log_file)
        elif db == 'couchdb':
            db_process = start_couchdb(log_file)
            remove_couchdb_files(log_file)
            kill_couchdb(db_process, log_file)


def ycsb_command(com_type, ex, db, th, wl):
    global cfg

    if cfg['mode'] == 'remote':
        host_ip = cfg['server_ip']
    else:
        host_ip = '127.0.0.1'

    if db == 'angra':
        db_com = 'angra -p angra.host=' + host_ip
    elif db == 'couchdb':
        db_com = 'couchdb -p couchdb.hosts=' + host_ip
    elif db == 'mongodb':
        db_com = 'mongodb -p mongodb.url=mongodb://' + host_ip + \
                 ':27017/ycsb?w=1'
    elif db == 'mysql':
        db_com = 'jdbc -cp ' + \
            cfg["mysql_jar_location"] + ' ' + \
            '-p db.driver=com.mysql.jdbc.Driver ' +\
            '-p db.url=jdbc:mysql://' + host_ip + ':3306/ycsb ' +\
            '-p db.user=root ' +\
            '-p db.passwd=root'

    if cfg["target"] == 0:
        targ_com = ''
    else:
        targ_com = '-target ' + str(cfg["target"]) + ' '

    if cfg["recordcount"] == 0:
        rcd_com = ''
    else:
        rcd_com = ' -p recordcount=' + str(cfg["recordcount"]) + ' '

    if cfg["operationcount"] == 0:
        opr_com = ''
    else:
        opr_com = ' -p operationcount=' + str(cfg["operationcount"]) + ' '

    if com_type == 'load':
        retry_com = ' -p core_workload_insertion_retry_limit=' + \
            str(cfg["retry_limit_times"]) + \
            ' -p core_workload_insertion_retry_interval=' + \
            str(cfg["retry_interval_seconds"]) + ' '
    else:
        retry_com = ''

    command = cfg["ycsb_location"] + 'bin/ycsb ' + com_type + \
        ' ' + db_com + ' -threads ' + str(th) + ' ' + \
        targ_com + rcd_com + opr_com + retry_com + '-s -P workloads/' + \
        wl + '> ' + log_types('result', ex, com_type, db, th, wl)

    return command


def start_process(log_file):
    global cfg

    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False, stdin=PIPE,
                 stdout=log_f, stderr=log_f)
    if cfg['mode'] == 'remote':
        comm = 'ssh -t -i ' + cfg["local_private_key"] + \
               ' ' + server_os_user + '@' + cfg['server_ip']
        proc.stdin.write(comm + '\n')
    proc.stdin.write('sudo ls' + '\n')
    # proc.stdin.write(server_os_pass + '\n')
    log_f.close()

    return proc


# couchdb
def start_couchdb(log_file):
    print time_stamp(), 'start CouchDB (start)'
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    proc.stdin.write('sudo service couchdb start' + '\n')
    log_f.close()
    sleep(3)
    print time_stamp(), 'start CouchDB (end)'
    return proc


def remove_couchdb_files(log_file):
    print time_stamp(), 'Clean CouchDB (start)'
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    proc.stdin.write(
        'curl -X DELETE http://admin:admin@localhost:5984/usertable' + '\n')
    proc.stdin.write('exit' + '\n')
    log_f.close()
    sleep(1)
    print time_stamp(), 'Clean CouchDB (end)'


def kill_couchdb(proc, log_file):
    print time_stamp(), 'stop CouchDB (start)'
    log_f = open(log_file, 'a')
    proc.stdin.write('sudo service couchdb stop' + '\n')
    proc.stdin.write('exit' + '\n')
    log_f.close()
    print time_stamp(), 'stop CouchDB (end)'

# mysql


def start_mysql(log_file):
    print time_stamp(), 'start MySQL (start)'
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    proc.stdin.write('sudo service mysql start' + '\n')
    log_f.close()
    sleep(3)
    print time_stamp(), 'start MySQL (end)'
    return proc


def remove_mysql_files(log_file):
    print time_stamp(), 'Clean MySQL (start)'
    # script_file = file('script.sql', 'w')
    # script_file.write('DROP DATABASE IF EXISTS ycsb;' + '\n')
    # script_file.write('CREATE DATABASE ycsb;' + '\n')
    # script_file.write('USE ycsb;' + '\n')
    # script_file.write('CREATE TABLE usertable (YCSB_KEY varchar(255),' +
    #                   'FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT,' +
    #                   'FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT,' +
    #                   'FIELD8 TEXT, FIELD9 TEXT, PRIMARY KEY (YCSB_KEY));' +
    #                   '\n')
    # script_file.close()
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    proc.stdin.write('mysql -u root -proot' + '\n')
    sleep(1)
    proc.stdin.write('DROP DATABASE IF EXISTS ycsb;' + '\n')
    proc.stdin.write('CREATE DATABASE ycsb;' + '\n')
    proc.stdin.write('USE ycsb;' + '\n')
    proc.stdin.write('CREATE TABLE usertable (YCSB_KEY varchar(255),' + '\n')
    proc.stdin.write('FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT,' +
                     '\n')
    proc.stdin.write('FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT,' +
                     '\n')
    proc.stdin.write('FIELD8 TEXT, FIELD9 TEXT, PRIMARY KEY (YCSB_KEY));' +
                     '\n')
    # proc.stdin.write('sudo rm -rf script.sql' + '\n')
    sleep(0.1)
    proc.stdin.write('exit' + '\n')
    log_f.close()
    print time_stamp(), 'Clean MySQL (end)'


def kill_mysql(proc, log_file):
    print time_stamp(), 'stop MySQL (start)'
    log_f = open(log_file, 'a')
    proc.stdin.write('sudo service mysql stop' + '\n')
    proc.stdin.write('exit' + '\n')
    log_f.close()
    print time_stamp(), 'stop MySQL (end)'


# angra
def start_angra(log_file):
    global cfg

    print time_stamp(), 'start Angra-DB (start)'
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    proc.stdin.write('cd ' + cfg["angra_core_location"] + '\n')
    sleep(0.1)
    proc.stdin.write(cfg["rebar3_command"] + ' shell' + '\n')
    sleep(5)
    proc.stdin.write('adb_app:kickoff(all).' + '\n')
    log_f.close()
    print time_stamp(), 'start Angra-DB (end)'
    return proc


def remove_angra_files(log_file):
    global cfg

    print time_stamp(), 'Clean Angra-DB (start)'
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    if cfg["angra_store_type"] == "adbtree":
        proc.stdin.write('rm -rf ' + cfg["angra_core_location"] +
                         'ycsb' + 'Docs.adb' + '\n')
        proc.stdin.write('rm -rf ' + cfg["angra_core_location"] +
                         'ycsb' + 'Index.adb' + '\n')
    elif cfg["angra_store_type"] == "hanoidb":
        proc.stdin.write('rm -rf ' + cfg["angra_core_location"] +
                         'ycsb' + '\n')
    log_f.close()
    print time_stamp(), 'Clean Angra-DB (end)'


def kill_angra(proc, log_file):
    print time_stamp(), 'stop Angra-DB (start)'
    log_f = open(log_file, 'a')
    proc.stdin.write('q().' + '\n')
    proc.stdin.write('exit' + '\n')
    log_f.close()
    print time_stamp(), 'stop Angra-DB (end)'


# mongodb
def start_mongodb(log_file):
    print time_stamp(), 'start MongoDB (start)'
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    proc.stdin.write('sudo service mongod start' + '\n')
    sleep(3)
    log_f.close()
    print time_stamp(), 'start MongoDB (end)'
    return proc


def remove_mongodb_files(log_file):
    print time_stamp(), 'Clean MongoDB (start)'
    proc = start_process(log_file)
    log_f = open(log_file, 'a')
    proc.stdin.write('mongo' + '\n')
    sleep(1)
    proc.stdin.write('use ycsb' + '\n')
    sleep(0.5)
    proc.stdin.write('db.dropDatabase() ' + '\n')
    sleep(2)
    proc.stdin.write('exit ' + '\n')
    sleep(0.2)
    proc.stdin.write('exit ' + '\n')
    log_f.close()
    # os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    print time_stamp(), 'Clean MongoDB (end)'


def kill_mongodb(proc, log_file):
    print time_stamp(), 'stop MongoDB (start)'
    log_f = open(log_file, 'a')
    proc.stdin.write('sudo service mongod stop' + '\n')
    log_f.close()
    print time_stamp(), 'stop MongoDB (end)'


def exectute_tests():
    global cfg

    log_file = cfg["ycsb_results_location"] + time_stamp_file() + '.log'

    init_used_databases(log_file)

    for database in cfg["dbs"]:
        for th in cfg["threads"]:
            for ex in range(1, cfg["executions"] + 1):
                for workload in cfg["workloads"]:
                    if database == 'angra':
                        db_process = start_angra(log_file)
                    elif database == 'mongodb':
                        db_process = start_mongodb(log_file)
                    elif database == 'mysql':
                        db_process = start_mysql(log_file)
                        sleep(1)
                        remove_mysql_files(log_file)
                    elif database == 'couchdb':
                        db_process = start_couchdb(log_file)
                    for com_type in ['load', 'run']:
                        command = ycsb_command(com_type, ex, database,
                                               th, workload)

                        log_print = log_types('screen', ex, com_type,
                                              database, th, workload)
                        print log_print
                        log_f = open(log_file, 'a')
                        log_f.write('\n' + log_print + '\n')
                        # ycsb_proc = Popen(['/bin/bash'], shell=False,
                        # stdin=PIPE, stdout=log_f)
                        # ycsb_proc.stdin.write(command + '\n')
                        ycsb_out = check_output(
                            command, cwd=cfg["ycsb_location"], shell=True,
                            stderr=subprocess.STDOUT)

                        log_f.write(ycsb_out)
                        log_f.close()
                    if database == 'angra':
                        kill_angra(db_process, log_file)
                        remove_angra_files(log_file)
                    elif database == 'mongodb':
                        remove_mongodb_files(log_file)
                        kill_mongodb(db_process, log_file)
                    elif database == 'mysql':
                        kill_mysql(db_process, log_file)
                    elif database == 'couchdb':
                        remove_couchdb_files(log_file)
                        kill_couchdb(db_process, log_file)


def read_result_files():
    cfg = load_config()
    overall_list = []
    gc_totals_list = []
    unknow_list = []
    operations_list = []
    totals_list = []

    print time_stamp(), 'reading files (start)'
    for file_type in ['load', 'run']:
        for db in cfg["dbs"]:
            for th in cfg["threads"]:
                for ex in range(1, cfg["executions"] + 1):
                    for wl in cfg["workloads"]:
                        file_name = log_types('result', ex, file_type, db,
                                              th, wl)
                        with open(file_name, 'r') as csvfile:
                            result_file = csv.reader(csvfile, delimiter=',')
                            for row in result_file:
                                read_line_from_result(db, str(ex), str(th),
                                                      wl, file_type, row,
                                                      operations_list,
                                                      totals_list,
                                                      overall_list,
                                                      gc_totals_list,
                                                      unknow_list)

    print time_stamp(), 'reading files (end)'

    unknow_list = clean_unkow(unknow_list)

    export_cvs_files('csv',
                     [
                         unknow_list, overall_list, gc_totals_list,
                         operations_list, totals_list
                     ],
                     [
                         'unknow', 'overall', 'GC_totals',
                         'operations', 'totals'
                     ],
                     cfg["ycsb_results_location"])


def read_line_from_result(db, ex, th, wl, stage, row, lst_ops, lst_totals,
                          lst_overall, lst_t_gc, lst_unknow):

    empty_col = ''

    if row[0] in ('[READ]', '[READ-FAILED]', '[UPDATE]', '[UPDATE-FAILED]',
                  '[INSERT]', '[CLEANUP]', '[INSERT-FAILED]'):

        if str(row[1].strip()) in ['Operations',
                                   'AverageLatency(us)',
                                   'MinLatency(us)',
                                   'MaxLatency(us)',
                                   '95thPercentileLatency(us)',
                                   '99thPercentileLatency(us)',
                                   'Return=OK',
                                   'Return=NOT_FOUND',
                                   'Return=UNEXPECTED_STATE']:

            if not lst_totals:
                lst_totals.append(
                    ['Execution', 'Database', 'Threads',
                     'Workload', 'Stage', 'Operation',
                     '# Operations', 'AverageLatency(us)',
                     'MinLatency(us)', 'MaxLatency(us)',
                     '95thPercentileLatency(us)', '99thPercentileLatency(us)',
                     'Return=OK', 'Return=NOT_FOUND',
                     'Return=UNEXPECTED_STATE']
                )

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
                     empty_col, empty_col,
                     empty_col, empty_col, empty_col, empty_col]
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

        elif (is_number(row[1].strip()) and is_number(row[2].strip())):
            if not lst_ops:
                lst_ops.append(['Execution',
                                'Database',
                                'Threads',
                                'Workload',
                                'Stage',
                                'Operation',
                                'Latency (us)', 'Frequence'])

            lst_ops.append([ex, db, th, wl, stage,
                            row[0].replace('[', '').replace(']', ''),
                            'error', 'error'])

            lst_ops[-1][6] = str(row[1].strip())
            lst_ops[-1][7] = str(row[2].strip())
        else:
            lst_unknow.append(''.join(row))

    elif '[TOTAL_GC' in row[0]:

        if not lst_t_gc:
            lst_t_gc.append(['Execution', 'Database', 'Threads',
                             'Workload', 'Stage',
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
                             'GC Time % [Time(%)]'])

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
                             empty_col, empty_col, empty_col])

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
        else:
            lst_unknow.append(''.join(row))

    elif row[0] == '[OVERALL]':

        if not lst_overall:
            lst_overall.append(['Execution', 'Database', 'Threads',
                                'Workload', 'Stage',
                                'RunTime(ms)',
                                'Throughput(ops/sec)'])

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


def export_cvs_files(sufix, lists,
                     file_name_list, ycsb_results_location):
    print time_stamp(), 'Creating', sufix, 'files (start)'
    for i in range(0, len(lists)):
        if len(lists[i]) > 1:
            new_file = open(ycsb_results_location + file_name_list[i] +
                            '.' + sufix, 'w')
            for row in lists[i]:
                if i == 0:  # uknow list
                    new_file.write(''.join(row) + '\n')
                else:  # another lists
                    new_file.write(';'.join(row) + '\n')
            new_file.close()

    print time_stamp(), 'Creating', sufix, 'files (start)'


def main(arg):
    global cfg
    cfg = load_config()

    if arg[0] == 'all':
        execute_test = True
        make_csv = True

    elif arg[0] == 'tests':
        execute_test = True
        make_csv = False

    elif arg[0] == 'csv':
        execute_test = False
        make_csv = True
    elif arg[0] == 'pwd':
        print os.getcwd()
        execute_test = False

    if execute_test:
        cfg = load_config()
        if cfg['mode'] == 'remote':
            print 'Remote mode, please type remote O.S. admin user\n'
            global server_os_user
            server_os_user = cfg['server_os_user']
            # raw_input('remote O.S. user: ')

        exectute_tests()

    if make_csv:
        read_result_files()


if __name__ == "__main__":
    main(sys.argv[1:])
