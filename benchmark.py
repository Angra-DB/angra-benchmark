from subprocess import Popen, PIPE, check_output
from time import sleep
import csv
import sys, os
import json
import subprocess

ON_POSIX = 'posix' in sys.builtin_module_names


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
    cfg = load_config()
    if type == 'result':
        to_return = cfg["ycsb_results_location"] + str(ex) + '-' + \
            com_type + '-' + db + '-threads-' + str(th) + '-' +\
            wl + '.log'
    elif type == 'screen':
        to_return = time_stamp() + ' - Running ' + com_type + ' number ' + \
            str(ex) + ' for ' + db + ' with ' + str(th) + ' threads of workload ' + \
            wl

    return to_return


# couchdb
def start_couchdb(log_file):
    print time_stamp(), 'start CouchDB (start)'
    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False, stdin=PIPE, stdout=log_f, stderr=log_f)
    proc.stdin.write('sudo service couchdb start' + '\n')
    log_f.close()
    sleep(3)
    print time_stamp(), 'start CouchDB (end)'
    return proc


def remove_couchdb_files(log_file):
    print time_stamp(), 'Clean CouchDB (start)'
    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False, stdin=PIPE, stdout=log_f, stderr=log_f)
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
    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False,
                 stdin=PIPE, stdout=log_f, stderr=log_f)
    proc.stdin.write('sudo service mysql start' + '\n')
    log_f.close()
    sleep(3)
    print time_stamp(), 'start MySQL (end)'
    return proc


def remove_mysql_files(log_file):
    print time_stamp(), 'Clean MySQL (start)'
    script_file = file('script.sql', 'w')
    script_file.write('DROP DATABASE IF EXISTS ycsb;' + '\n')
    script_file.write('CREATE DATABASE ycsb;' + '\n')
    script_file.write('USE ycsb;' + '\n')
    script_file.write('CREATE TABLE usertable (YCSB_KEY varchar(255),' +
                      'FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT,' +
                      'FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT,' +
                      'FIELD8 TEXT, FIELD9 TEXT, PRIMARY KEY (YCSB_KEY));' + '\n')
    script_file.close()
    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False,
                 stdin=PIPE, stdout=log_f, stderr=log_f)
    proc.stdin.write('mysql -u root -proot < script.sql' + '\n')
    sleep(2)
    proc.stdin.write('sudo rm -rf script.sql' + '\n')
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


def start_angra(angra_core_location, rebar3_command, log_file):
    print time_stamp(), 'start Angra-DB (start)'
    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False,
                 stdin=PIPE, stdout=log_f, stderr=log_f)
    proc.stdin.write('cd ' + angra_core_location + '\n')
    sleep(0.1)
    proc.stdin.write(rebar3_command + ' shell' + '\n')
    sleep(3)
    proc.stdin.write('adb_app:kickoff(all).' + '\n')
    log_f.close()
    print time_stamp(), 'start Angra-DB (end)'
    return proc


def remove_angra_files(angra_core_location, log_file):
    print time_stamp(), 'Clean Angra-DB (start)'
    log_f = open(log_file, 'a')
    out = check_output(['rm', '-rf', angra_core_location +
                        'ycsb' + 'Docs.adb'], stderr=subprocess.STDOUT)
    log_f.write(out + '\n')
    out = check_output(['rm', '-rf', angra_core_location +
                        'ycsb' + 'Index.adb'], stderr=subprocess.STDOUT)
    log_f.write(out + '\n')
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
    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False, stdin=PIPE, stdout=log_f, stderr=log_f)
    proc.stdin.write('sudo service mongod start' + '\n')
    sleep(3)
    log_f.close()
    print time_stamp(), 'start MongoDB (end)'
    return proc


def remove_mongodb_files(log_file):
    print time_stamp(), 'Clean MongoDB (start)'
    log_f = open(log_file, 'a')
    proc = Popen(['/bin/bash'], shell=False, stdin=PIPE, stdout=log_f, stderr=log_f)
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
    #os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    print time_stamp(), 'Clean MongoDB (end)'


def kill_mongodb(proc, log_file):
    print time_stamp(), 'stop MongoDB (start)'
    log_f = open(log_file, 'a')
    proc.stdin.write('sudo service mongod stop' + '\n')
    log_f.close()
    print time_stamp(), 'stop MongoDB (end)'


def exectute_tests():
    cfg = load_config()
    angra_core_location = cfg["angra_core_location"]
    rebar3_command = cfg["rebar3_command"]
    ycsb_location = cfg["ycsb_location"]
    log_file = cfg["ycsb_results_location"] + time_stamp_file() + '.log'

    if cfg["target"] == 0:
        targ_com = ''
    else:
        targ_com = '-target ' + str(cfg["target"]) + ' '

    if cfg["recordcount"] == 0:
        rcd_com = ''
    else:
        rcd_com = '-p recordcount=' + str(cfg["recordcount"]) + ' '

    if cfg["operationcount"] == 0:
        opr_com = ''
    else:
        opr_com = '-p operationcount=' + str(cfg["operationcount"]) + ' '

    db_process = start_angra(angra_core_location, rebar3_command, log_file)
    kill_angra(db_process, log_file)
    remove_angra_files(angra_core_location, log_file)

    db_process = start_mongodb(log_file)
    remove_mongodb_files(log_file)
    kill_mongodb(db_process, log_file)

    db_process = start_mysql(log_file)
    remove_mysql_files(log_file)
    kill_mysql(db_process, log_file)

    db_process = start_couchdb(log_file)
    remove_couchdb_files(log_file)
    kill_couchdb(db_process, log_file)

    for database in cfg["dbs"]:
        for th in cfg["threads"]:
            for ex in range(1, cfg["executions"] + 1):
                for workload in cfg["workloads"]:
                    if database == 'angra':
                        db_com = 'angra'
                        db_process = start_angra(
                            angra_core_location, rebar3_command, log_file)
                    elif database == 'mongodb':
                        db_com = 'mongodb'
                        db_process = start_mongodb(log_file)
                    elif database == 'mysql':
                        db_com = 'jdbc -cp ' + \
                            cfg["mysql_jar_location"] + ' ' + \
                            '-p db.driver=com.mysql.jdbc.Driver ' +\
                            '-p db.url=jdbc:mysql://127.0.0.1:3306/ycsb ' +\
                            '-p db.user=root ' +\
                            '-p db.passwd=root'
                        db_process = start_mysql(log_file)
                        sleep(1)
                        remove_mysql_files(log_file)
                    elif database == 'couchdb':
                        db_com = 'couchdb'
                        db_process = start_couchdb(log_file)

                    for com_type in ['load', 'run']:
                        command = ycsb_location + \
                            'bin/ycsb ' + \
                            com_type + \
                            ' ' + db_com + ' ' + \
                            '-threads ' + str(th) + ' ' +\
                            targ_com +\
                            rcd_com +\
                            opr_com +\
                            ' -s -P workloads/' + \
                            workload + \
                            '> ' + \
                            log_types('result', ex, com_type, database, th,
                                                    workload)

                        log_print =  log_types('screen', ex, com_type, database,
                                              th, workload)
                        print log_print
                        log_f = open(log_file, 'a')
                        log_f.write('\n' + log_print + '\n')
                        # ycsb_proc = Popen(['/bin/bash'], shell=False, stdin=PIPE, stdout=log_f)
                        # ycsb_proc.stdin.write(command + '\n')
                        ycsb_out = check_output(
                            command, cwd=ycsb_location, shell=True, stderr=subprocess.STDOUT)

                        log_f.write(ycsb_out)
                        log_f.close()
                    if database == 'angra':
                        kill_angra(db_process, log_file)
                        remove_angra_files(angra_core_location, log_file)
                    elif database == 'mongodb':
                        remove_mongodb_files(log_file)
                        kill_mongodb(db_process, log_file)
                    elif database == 'mysql':
                        kill_mysql(db_process, log_file)
                    elif database == 'couchdb':
                        remove_couchdb_files(log_file)
                        kill_couchdb(db_process, log_file)


def read_result_files(file_type):
    cfg = load_config()
    overall_list = []
    gc_totals_list = []
    unknow_list = []

    cleanup_list = []
    cleanup_totals_list = []

    insert_list = []
    insert_totals_list = []

    read_list = []
    read_totals_list = []

    update_list = []
    update_totals_list = []

    read_failed_list = []
    read_failed_totals_list = []

    insert_failed_list = []
    insert_failed_totals_list = []

    print 'reading', file_type, 'files (start)'

    for database in cfg["dbs"]:
        for th in cfg["threads"]:
            for ex in range(1, cfg["executions"] + 1):
                for workload in cfg["workloads"]:
                    file_name = log_types('result', ex, file_type, database,
                                          th, workload)
                    with open(file_name, 'r') as csvfile:
                        result_file = csv.reader(csvfile, delimiter=',')
                        for row in result_file:
                            read_line_from_result(str(ex), row,
                                                  overall_list, gc_totals_list,
                                                  unknow_list, cleanup_list,
                                                  cleanup_totals_list,
                                                  insert_list,
                                                  insert_totals_list,
                                                  read_list, read_totals_list,
                                                  update_list,
                                                  update_totals_list,
                                                  read_failed_list,
                                                  read_failed_totals_list,
                                                  insert_failed_list,
                                                  insert_failed_totals_list)


        print 'reading', file_type, 'files (end)'
        export_cvs_files(database,
                         file_type,
                         'csv',
                         [
                             overall_list, gc_totals_list, unknow_list,
                             cleanup_list, cleanup_totals_list,
                             insert_list, insert_totals_list,
                             read_list, read_totals_list,
                             update_list, update_totals_list,
                             read_failed_list, read_failed_totals_list,
                             insert_failed_list, insert_failed_totals_list
                         ],
                         [
                             'overall', 'GC_totals', 'unknow',
                             'cleanup', 'cleanup_totals',
                             'insert', 'insert_totals',
                             'read', 'read_totals',
                             'update', 'update_totals',
                             'read_failed', 'read_failed_totals',
                             'insert_failed', 'insert_failed_totals'
                         ],
                         cfg["ycsb_results_location"]
                         )

        overall_list = []
        gc_totals_list = []
        unknow_list = []

        cleanup_list = []
        cleanup_totals_list = []

        insert_list = []
        insert_totals_list = []

        read_list = []
        read_totals_list = []

        update_list = []
        update_totals_list = []

        read_failed_list = []
        read_failed_totals_list = []

        insert_failed_list = []
        insert_failed_totals_list = []


def read_line_from_result(ex, row,
                          lst_overall, lst_t_gc, lst_unknow,
                          lst_cleanup, lst_t_cleanup,
                          lst_insert, lst_t_insert,
                          lst_read, lst_t_read,
                          lst_update, lst_t_update,
                          lst_read_fail, lst_t_read_fail,
                          lst_insert_fail, lst_t_insert_fail):

    empty_col = ''

    if row[0] in ('[READ]',
                  '[READ-FAILED]',
                  '[UPDATE]', '[INSERT]', '[CLEANUP]', '[INSERT-FAILED]'):
        if row[0] == '[READ]':
            lst = lst_read
            lst_t = lst_t_read
        elif row[0] == '[READ-FAILED]':
            lst = lst_read_fail
            lst_t = lst_t_read_fail
        elif row[0] == '[UPDATE]':
            lst = lst_update
            lst_t = lst_t_update
        elif row[0] == '[INSERT]':
            lst = lst_insert
            lst_t = lst_t_insert
        elif row[0] == '[CLEANUP]':
            lst = lst_cleanup
            lst_t = lst_t_cleanup
        elif row[0] == '[INSERT-FAILED]':
            lst = lst_insert_fail
            lst_t = lst_t_insert_fail

        if str(row[1].strip()) in ['Operations',
                                   'AverageLatency(us)',
                                   'MinLatency(us)',
                                   'MaxLatency(us)',
                                   '95thPercentileLatency(us)',
                                   '99thPercentileLatency(us)',
                                   'Return=OK',
                                   'Return=NOT_FOUND']:

            if not lst_t:
                lst_t.append(
                    ['Execution', 'Operations', 'AverageLatency(us)',
                     'MinLatency(us)', 'MaxLatency(us)',
                     '95thPercentileLatency(us)', '99thPercentileLatency(us)',
                     'Return=OK', 'Return=NOT_FOUND']
                )

            if not [item for item in lst_t if item[0] == ex]:
                lst_t.append(
                    [ex, empty_col, empty_col, empty_col,
                     empty_col, empty_col,
                     empty_col, empty_col, empty_col]
                )

            if str(row[1].strip()) == 'Operations':
                lst_t[-1][1] = str(row[2].strip())
            elif str(row[1].strip()) == 'AverageLatency(us)':
                lst_t[-1][2] = str(row[2].strip())
            elif str(row[1].strip()) == 'MinLatency(us)':
                lst_t[-1][3] = str(row[2].strip())
            elif str(row[1].strip()) == 'MaxLatency(us)':
                lst_t[-1][4] = str(row[2].strip())
            elif str(row[1].strip()) == '95thPercentileLatency(us)':
                lst_t[-1][5] = str(row[2].strip())
            elif str(row[1].strip()) == '99thPercentileLatency(us)':
                lst_t[-1][6] = str(row[2].strip())
            elif str(row[1].strip()) == 'Return=OK':
                lst_t[-1][7] = str(row[2].strip())
            elif str(row[1].strip()) == 'Return=NOT_FOUND':
                lst_t[-1][8] = str(row[2].strip())
        else:
            if not lst:
                lst.append(['Execution', 'micro second', 'quant'])

            lst.append([ex, 'error', 'error'])
            lst[-1][1] = str(row[1].strip())
            lst[-1][2] = str(row[2].strip())

    elif '[TOTAL_GC' in row[0]:

        if not lst_t_gc:
            lst_t_gc.append(['Execution',
                             'GCS_Copy [Count]',
                             'GC Time Copy [Time(ms)]',
                             'GC Time % Copy [Time(%)]',
                             'GCS MarkSweepCompact [Count]',
                             'GC Time MarkSweepCompact [Time(ms)]',
                             'GC Time % MarkSweepCompact [Time(%)]',
                             'GCs [Count]',
                             'GC Time [Time(ms)]',
                             'GC Time % [Time(%)]'])

        if not [item for item in lst_t_gc if item[0] == ex]:
            lst_t_gc.append([ex, empty_col, empty_col, empty_col, empty_col,
                             empty_col, empty_col, empty_col, empty_col,
                             empty_col])

        if str(row[0].strip()) == '[TOTAL_GCS_Copy]':
            lst_t_gc[-1][1] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GC_TIME_Copy]':
            lst_t_gc[-1][2] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_Copy]':
            lst_t_gc[-1][3] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GCS_MarkSweepCompact]':
            lst_t_gc[-1][4] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GC_TIME_MarkSweepCompact]':
            lst_t_gc[-1][5] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GC_TIME_%_MarkSweepCompact]':
            lst_t_gc[-1][6] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GCs]':
            lst_t_gc[-1][7] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GC_TIME]':
            lst_t_gc[-1][8] = str(row[2].strip())
        elif str(row[0].strip()) == '[TOTAL_GC_TIME_%]':
            lst_t_gc[-1][8] = str(row[2].strip())
    elif row[0] == '[OVERALL]':

        if not lst_overall:
            lst_overall.append(['Execution',
                                'RunTime(ms)',
                                'Throughput(ops/sec)'])

        if not [item for item in lst_overall if item[0] == ex]:
            lst_overall.append([ex, empty_col, empty_col])

        if str(row[1].strip()) == 'RunTime(ms)':
            lst_overall[-1][1] = str(row[2].strip())
        elif str(row[1].strip()) == 'Throughput(ops/sec)':
            lst_overall[-1][2] = str(row[2].strip())
    else:
        lst_unknow.append(''.join(row))


def export_cvs_files(database, prefix, sufix, lists, file_name_list, ycsb_results_location):
    print 'Creating', prefix, sufix, 'files (start)'
    for i in range(0, len(lists)):
        if len(lists[i]) > 1:
            new_file = open(ycsb_results_location + database + '_' + prefix + '_' +
                            file_name_list[i] + '.' + sufix, 'w'
                           )
            for row in lists[i]:
                new_file.write(';'.join(row) + '\n')
            new_file.close()

    print 'Creating', prefix, sufix, 'files (end)'


def main(arg):
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
	make_csv = False

    if execute_test:
        exectute_tests()
    if make_csv:
        for file_type in ['run', 'load']:
            read_result_files(file_type)


if __name__ == "__main__":
    main(sys.argv[1:])
