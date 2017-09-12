import os, re, sys, shutil, glob, time, csv, xlsxwriter
import argparse as ap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')
import msvcrt as m

version = "VNX Onions for MS Windows 0.1"
# col_filter = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,55,56,57,58]
col_filter = ['Poll Time','Object Name','Owner Array Name','Current Owner','Utilization (%)',
                'Utilization-Optimal (%)','Utilization-Nonoptimal (%)','Queue Length','Queue Length-Optimal',
                'Queue Length-Nonoptimal','Response Time (ms)','Response Time-Optimal (ms)','Response Time-Nonoptimal (ms)',
                'Total Bandwidth (MB/s)','Total Bandwidth-Optimal (MB/s)','Total Bandwidth-Nonoptimal (MB/s)','Total Throughput (IO/s)',
                'Read Bandwidth (MB/s)','Read Bandwidth-Optimal (MB/s)','Read Bandwidth-Nonoptimal (MB/s)','Read Size (KB)','Read Size-Optimal (KB)','Read Size-Nonoptimal (KB)','Read Throughput (IO/s)','Read Throughput-Optimal (IO/s)','Read Throughput-Nonoptimal (IO/s)','Write Bandwidth (MB/s)','Write Bandwidth-Optimal (MB/s)','Write Bandwidth-Nonoptimal (MB/s)','Write Size (KB)','Write Size-Optimal (KB)','Write Size-Nonoptimal (KB)','Write Throughput (IO/s)','Write Throughput-Optimal (IO/s)','Write Throughput-Nonoptimal (IO/s)','Service Time (ms)','Service Time-Optimal (ms)','Service Time-Nonoptimal (ms)']

#################################### Arguments and settings

parser = ap.ArgumentParser(description=version + ' Download & analyse VNX .nar files')
parser.add_argument('-nar',dest='nar_folder', required = False, help='NAR files location')
parser.add_argument('-u',dest='user', required = False, help='Unisphere user')
parser.add_argument('-p',dest='password', required = False, help='Unisphere password')
parser.add_argument('-s',dest='scope', required = False, help='Unisphere scope')
parser.add_argument('-ip',dest='IP', required = False, help='SPA or SPB IP address')
parser.add_argument('-f',dest='from_date', required = False, help='Start date YYYY-MM-DD')
parser.add_argument('-t',dest='to_date', required = False, help='End date YYYY-MM-DD')
parser.add_argument('-cd',dest='description', required = False, help='Description (String)')
parser.add_argument('--no-graphs', dest='nographs',required = False, action="store_const", const = True, help='No graphs')
parser.add_argument('-of',dest='outfile_location', required = False, help='(outfile location)')
parser.add_argument('-dpi',dest='dpi', required = False, help='resolution in DPI')

args = parser.parse_args()

if args.nar_folder is None and args.outfile_location is None and not all([args.user,args.password,args.IP,args.from_date,args.to_date,args.scope]):
    print "\nError: Please specify NAR folder OR VNX/CX array IP and credentials\n"
    parser.print_help()
    sys.exit()


user = args.user
password = args.password
IP = args.IP
from_date = args.from_date
to_date = args.to_date
scope = args.scope
description = args.description
nar_folder = args.nar_folder

try:
    args.dpi = float(args.dpi)
except TypeError:
    pass


working_folder = None

if nar_folder:
    working_folder = nar_folder
elif args.outfile_location:
    working_folder = os.path.split(args.outfile_location)[0]
else:
    pass


serial = None

start_time = time.strftime("%H:%M:%S")

#################################### NAR GETTER

def get_nar_file_list(IP,user,password,scope):
    print "Querying array for performance archives ..."
    os.system('naviseccli.exe'+' -h '+ IP +' -scope '+ scope +' -user '+ user +' -password ' + password + ' analyzer -archive -list > nar.list')

def parse_nars(from_date,to_date):
    print "Parsing the archive list ..."
    nar_list_raw = open('nar.list','r')
    nar_list = []
    nar_list_selected = []
    for row in nar_list_raw:
        parts = row.split()
        nar_list.append(parts[4])
    del nar_list[0]
    for nar_filename in nar_list:
        date = int(((nar_filename.split('_'))[2]).translate(None,'-'))
        if date >= int(from_date.translate(None,'-')) and date <= int(to_date.translate(None,'-')):
            nar_list_selected.append(nar_filename)
    return nar_list_selected

def get_nars(nar_file_list):
    print "Downloading the archive files ..."
    serial = (nar_file_list[1].split('_'))[0]
    os.system('mkdir ' + serial)                                # create a directory with an array SN
    for nar_file in nar_file_list:                              # download the files
        print ('downloading ' + nar_file)
        os.system('naviseccli.exe'+' -h '+ IP +' -scope ' + scope +' -user '+ user +' -password ' + password + ' analyzer -archive -file ' + nar_file + ' -o')
    for filename in glob.glob(serial + '*.nar'):
        try:
            shutil.move(filename, '.\\'+ serial + '\\')
        except shutil.Error:
            print 'File %s already exists, skipping ...' % (nar_file)
    try:
        os.remove('nar.list')
    except WindowsError:
        pass
    return serial

def decrypt_nars():
    print "Decrypting archive files ..."
    for filename in glob.iglob('*.nar'):
        print "Now decrypting %s" % (filename)
        os.system('naviseccli.exe analyzer -archivedump -data ' + filename + ' -out ' + filename + '.csv')

if nar_folder is None and args.outfile_location is None:
    script_mode = "array"
    print "Direct array download"
    get_nar_file_list(IP,user,password,scope)
    serial = get_nars(parse_nars(from_date,to_date))
    os.chdir('.\\'+ serial + '\\')
    decrypt_nars()
elif nar_folder is not None and args.outfile_location is None:
    script_mode = "nar"
    print "NAR folder specified, processing pre-downloaded NAR files..."
    os.chdir(nar_folder)
    decrypt_nars()
    pass
else:
    script_mode = "outfile"
    print "Outfile selected, processing pre-compiled outfile..."
    pass

#################################### MAIN start



try:
    os.remove('*outfile')
except WindowsError:
    pass

try:
    serial = str(list(glob.iglob('*.nar'))[0].split('_')[0])
except:
    serial = "serial_not_available"

if args.outfile_location is None:
    outfile = open(serial + '_' + str(from_date) + '-' + str(to_date) + '.ofl','a')
    print "Compiling main performance file: %s" % (outfile)
    filecount = 0
    for filename in glob.iglob('*.csv'):
        filecount += 1
        print "Now processing %s" % (filename)
        if filecount == 1:
            sn = filename.split('_')[0]
            date_from = filename.split('_')[2]
            infile = open(filename,'r')
            for line in infile:
                outfile.write(line)
        else:
            infile = open(filename,'r')
            infile.next()
            for line in infile:
                outfile.write(line)
# os.rename('outfile.csv',sn + '_' + date_from + '.csv')

############################################## data analytics


sanitize_dict = {' ': '_','(%)': 'pct','(': '',')': '','%': 'pct','/':'_per_'}

def nar2csv():
    print glob.iglob('*.nar')
    for filename in glob.iglob('*.nar'):
        print filename
        os.system('naviseccli.exe analyzer -archivedump -data ' + filename + ' -out ' + filename + '.csv')

def sanitize_cols(df):    #remove whitespaces and while at it, remove stupid characters
    print "Sanitizing the metrics names..."
    for bug,solution in sanitize_dict.items():
        df.columns = df.columns.str.replace(bug,solution)
        df.columns = df.columns.str.lower()
    print "Sanitizing done"
    return df

def get_obj(df):    #scans the object name column to retrieve the list of objects
    duped_lst = df.object_name.tolist()
    deduped_lst = []                        # all values that show in 'object name'
    for obj in duped_lst:                   # deduplication
        if obj not in deduped_lst:
            deduped_lst.append(obj)
    return deduped_lst

def rag_format(workbook,worksheet,cell_range,amber_threshold,red_threshold):

    # Add a format. Light red fill with dark red text.
    red = workbook.add_format({'bg_color': '#FFC7CE',
    'font_color': '#9C0006'})

    # Add a format. Yellow fill with dark yellow text.
    amber = workbook.add_format({'bg_color': '#f4e541',
    'font_color': '#f4af41'})

    # Add a format. Green fill with dark green text.
    green = workbook.add_format({'bg_color': '#C6EFCE',
    'font_color': '#006100'})

    worksheet.conditional_format(cell_range, {'type':     'cell',
    'criteria': '>',
    'value':    red_threshold,
    'format':   red})

    worksheet.conditional_format(cell_range, {'type':     'cell',
    'criteria': 'between',
    'minimum':    amber_threshold,
    'maximum':    red_threshold,
    'format':   amber})

    worksheet.conditional_format(cell_range, {'type':     'cell',
    'criteria': 'between',
    'minimum':    0,
    'maximum':    amber_threshold,
    'format':   green})

def obj_type(obj):
    obj_type = 'Unknown'
    if len(obj.split('[')) > 1:
        obj_type = 'LUN'
    if obj.startswith('Port'):
        obj_type = 'Port'
    elif obj.startswith('SP'):
        obj_type = 'SP'
    elif len(obj.split('Enclosure')) > 1:
        obj_type = 'Disk'
    return obj_type

if outfile is None:
    outfile = args.outfile_location

if working_folder is None:
    working_folder = ".\\" + serial + "\\"

# print "current folder is %s" % os.getcwd()
# print "working folder is %s" % working_folder
# os.chdir(working_folder) #change to the working folder for file generation



print "Loading the main DataFrame from %s, hang on - this takes time..." % (outfile)

timestamp = [1]
try:
    df = pd.read_csv(outfile.name,parse_dates=timestamp,usecols=col_filter)
except AttributeError:
    df = pd.read_csv(os.path.split(args.outfile_location)[1],parse_dates=timestamp,usecols=col_filter)

print 'Main DataFrame loaded'

# print list(df)

sanitize_cols(df)

obj_list = get_obj(df)

id_list = ['poll_time','object_name']

metrics_list_SP = ['utilization_pct',
                    'response_time_ms',
                    'service_time_ms',
                    'queue_length',
                    'read_bandwidth_mb_per_s',
                    'write_bandwidth_mb_per_s',
                    'read_size_kb',
                    'write_size_kb',
                    'read_throughput_io_per_s',
                    'write_throughput_io_per_s']


metrics_set_list_SP = [['utilization_pct'],
                    ['response_time_ms',
                    'service_time_ms'],
                    ['queue_length'],
                    ['write_size_kb',
                    'read_size_kb'],
                    ['read_bandwidth_mb_per_s',
                    'write_bandwidth_mb_per_s'],
                    ['read_throughput_io_per_s',
                    'write_throughput_io_per_s']]


sp_stats_select = ["utilization_pct_max","utilization_pct_mean","utilization_pct_95th",
    # "response_time_ms_max","response_time_ms_mean","response_time_ms_95th",
    # "service_time_ms_max","service_time_ms_mean","service_time_ms_95th",
    # "queue_length_max","queue_length_mean","queue_length_95th",
    "read_bandwidth_mb_per_s_max","read_bandwidth_mb_per_s_mean","read_bandwidth_mb_per_s_95th",
    "write_bandwidth_mb_per_s_max","write_bandwidth_mb_per_s_mean","write_bandwidth_mb_per_s_95th",
    # "read_size_kb_max","read_size_kb_mean","read_size_kb_95th","write_size_kb_max","write_size_kb_mean","write_size_kb_95th",
    "read_throughput_io_per_s_max","read_throughput_io_per_s_mean","read_throughput_io_per_s_95th",
    "write_throughput_io_per_s_max","write_throughput_io_per_s_mean","write_throughput_io_per_s_95th"]

lun_stats_select = [

    # "utilization_pct_max","utilization_pct_mean","utilization_pct_95th",
    "response_time_ms_max","response_time_ms_mean","response_time_ms_95th",
    # "service_time_ms_max","service_time_ms_mean","service_time_ms_95th",
    # "queue_length_max","queue_length_mean","queue_length_95th",
    "read_bandwidth_mb_per_s_max","read_bandwidth_mb_per_s_mean","read_bandwidth_mb_per_s_95th",
    "write_bandwidth_mb_per_s_max","write_bandwidth_mb_per_s_mean","write_bandwidth_mb_per_s_95th",
    # "read_size_kb_max","read_size_kb_mean","read_size_kb_95th","write_size_kb_max","write_size_kb_mean","write_size_kb_95th",
    "read_throughput_io_per_s_max","read_throughput_io_per_s_mean","read_throughput_io_per_s_95th",
    "write_throughput_io_per_s_max","write_throughput_io_per_s_mean","write_throughput_io_per_s_95th"]

disk_stats_select = [

    "utilization_pct_max","utilization_pct_mean","utilization_pct_95th",
    "response_time_ms_max","response_time_ms_mean","response_time_ms_95th",
    # "service_time_ms_max","service_time_ms_mean","service_time_ms_95th",
    # "queue_length_max","queue_length_mean","queue_length_95th",
    # "read_bandwidth_mb_per_s_max","read_bandwidth_mb_per_s_mean","read_bandwidth_mb_per_s_95th",
    # "write_bandwidth_mb_per_s_max","write_bandwidth_mb_per_s_mean","write_bandwidth_mb_per_s_95th",
    # "read_size_kb_max","read_size_kb_mean","read_size_kb_95th","write_size_kb_max","write_size_kb_mean","write_size_kb_95th",
    # "read_throughput_io_per_s_max","read_throughput_io_per_s_mean","read_throughput_io_per_s_95th",
    # "write_throughput_io_per_s_max","write_throughput_io_per_s_mean","write_throughput_io_per_s_95th"
    ]

# list of metrics measured for SP
column_list = id_list + metrics_list_SP

# print df[df['object_name'] == 'SP A'][metrics_list_SP]

util_dict = {}
for obj in obj_list:
    metric_df = df[df['object_name'] == obj]['utilization_pct']
    try:
        util_dict[obj] = metric_df.quantile(0.95)
    except TypeError:
        pass
    # print 'Object %s utilization: %s' % (obj.split('[')[0],metric_df.quantile(0.95))

util_dict = dict(sorted(util_dict.iteritems(), key=lambda (k,v): (v,k)))

# for obj in util_dict:
#     print '%s %s' % (obj.split('[')[0],util_dict[obj])

sorted_obj_list = util_dict.keys()

#guess array name (account and device) from LUN names

guessing_list = []

for item in sorted_obj_list:
    try:
        guessing_list.append(item.split('-')[0] + '-' + item.split('-')[1])
    except IndexError:
        pass


def most_common(lst):
    return max(set(lst), key=lst.count)

array_name = most_common(guessing_list)


# for key in reversed(sorted_obj_list):
#     print '%s is %s' % (key, obj_type(key))
extended_metrics_list = []
extended_metrics_list.append('Obj')
extended_metrics_list.append('Type')
for i in range(len(metrics_list_SP)):
    extended_metrics_list.append(metrics_list_SP[i] + '_max')
    extended_metrics_list.append(metrics_list_SP[i] + '_mean')
    extended_metrics_list.append(metrics_list_SP[i] + '_95th')

#OUTPUT OF CUMULATIVE DATA TO CSV
with open(serial + 'lun_stats_tab.csv','wb') as lun_stats_tab:
    csv_writer = csv.writer(lun_stats_tab)
    csv_writer.writerow(extended_metrics_list)
    for obj in reversed(sorted_obj_list):
        metric_df = df[df['object_name'] == obj][metrics_list_SP]
        stats_line = [obj.split('[')[0]] + [obj_type(obj)]
        for metric in metric_df:
            metric_max = metric_df[metric].max()
            metric_mean = metric_df[metric].mean()
            metric_95th = metric_df[metric].quantile(0.95)
            stats_line_part = [metric_max,metric_mean,metric_95th]
            stats_line = stats_line + stats_line_part
        csv_writer.writerow(stats_line)

print ("Summary CSV ready ...")
print ("Loading results into DataFrame ...")

stats_df = pd.read_csv(serial + 'lun_stats_tab.csv')
sp_stats = stats_df[stats_df['Type'] == 'SP'].sort_values('Obj')
lun_stats = stats_df[stats_df['Type'] == 'LUN'].sort_values('utilization_pct_95th',ascending=False,na_position='last')
disk_stats = stats_df[stats_df['Type'] == 'Disk'].sort_values('Obj')
# lun_stats_growth = df[
#                             df['object_name',
#                             df['read_bandwidth_mb_per_s_mean'].resample('W','first')*604800-df['read_bandwidth_mb_per_s_mean'].resample('W','last')*604800,
#                             df['write_bandwidth_mb_per_s_mean'].resample('W','first')*604800-df['write_bandwidth_mb_per_s_mean'].resample('W','last')*604800,
#                             df['read_throughput_mb_per_s_mean'].resample('W','first')*604800-df['read_throughput_mb_per_s_mean'].resample('W','last')*604800,
#                             df['write_throughput_mb_per_s_mean'].resample('W','first')*604800-df['write_throughput_mb_per_s_mean'].resample('W','last')*604800,
#                             ],
#                             #columns = ['LUN name','Read BW change','Write BW change','Read IOPS change','Write IOPS change']
#                             ]
# lun_stats_growth.columns(['LUN name','Read BW change','Write BW change','Read IOPS change','Write IOPS change'])
# import pdb; pdb.set_trace()
def lun_load_growth():
    LUN_list = lun_stats.Obj.tolist()
    lun_stats_growth = pd.DataFrame()
    # lun_stats_growth.columns(['LUN name','Read BW change','Write BW change','Read IOPS change','Write IOPS change'])
    for lun in LUN_list:
        lun_frame = df[df["object_name"] == lun]["object_name","read_bandwidth_mb_per_s","write_bandwidth_mb_per_s","read_throughput_mb_per_s","write_throughput_mb_per_s"]
        lun_growth_line = pd.DataFrame({'LUN name':[lun],
                                        'R BW growth':[(df[df["object_name"] == lun]["read_bandwidth_mb_per_s"].resample('1W','first') - df[df["object_name"] == lun]["read_bandwidth_mb_per_s"].resample('1W','last'))*604800],
                                        'W BW growth':[(df[df["object_name"] == lun]["write_bandwidth_mb_per_s"].resample('1W','first') - df[df["object_name"] == lun]["write_bandwidth_mb_per_s"].resample('1W','last'))*604800],
                                        'R IOPS growth':[(df[df["object_name"] == lun]["read_throughput_io_per_s"].resample('1W','first') - df[df["object_name"] == lun]["read_throughput_io_per_s"].resample('1W','last'))*604800],
                                        'W IOPS growth':[(df[df["object_name"] == lun]["write_throughput_io_per_s"].resample('1W','first') - df[df["object_name"] == lun]["write_throughput_io_per_s"].resample('1W','last'))*604800],
                                        })
        lun_stats_growth.append(lun_growth_line)
    return lun_stats_growth

# lun_load_growth()

print ("Exporting to MS Excel template ...")
try:
    template_writer = pd.ExcelWriter((str(df.poll_time.min())).split(' ')[0] + "-" + (str(df.poll_time.max())).split(' ')[0] +"_"+ array_name + '_stats.xlsx',engine='xlsxwriter')
except IOError:
    template_writer = pd.ExcelWriter('_generic_file_stats.xlsx',engine='xlsxwriter')

workbook  = template_writer.book
bold = workbook.add_format({'bold': True})
title = workbook.add_format({'bold': True, 'font_size': 48})

start_row = [4,10,24]

sp_stats[["Obj"]+sp_stats_select].to_excel(template_writer,'Dashboard',startrow=start_row[0] , startcol=0)
lun_stats[["Obj"]+lun_stats_select].head(10).to_excel(template_writer,'Dashboard',startrow=start_row[1] , startcol=0)
disk_stats[["Obj"]+disk_stats_select][disk_stats["utilization_pct_95th"]>70].to_excel(template_writer,'Dashboard',startrow=start_row[2] , startcol=0)

try:
    lun_stats_growth.to_excel(template_writer,'LUN load growth totals',startrow=start_row[0] , startcol=0)
except NameError:
    pass


worksheet = template_writer.sheets['Dashboard']
worksheet.set_column(1, 1, 40)
# Heading cell merge for SP part of dashboard
worksheet.merge_range('C'+str(start_row[0])+':E'+str(start_row[0]), 'Utilization (%)', bold)
worksheet.merge_range('F'+str(start_row[0])+':H'+str(start_row[0]), 'Read MBps', bold)
worksheet.merge_range('I'+str(start_row[0])+':K'+str(start_row[0]), 'Write MBps', bold)
worksheet.merge_range('L'+str(start_row[0])+':N'+str(start_row[0]), 'Read IOPS', bold)
worksheet.merge_range('O'+str(start_row[0])+':Q'+str(start_row[0]), 'Write IOPS', bold)
worksheet.write_row('C'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('F'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('I'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('L'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('O'+str(start_row[0]+1),('max','mean','95th'))
# Heading cell merge for LUN part of dashboard

worksheet.merge_range('C'+str(start_row[1])+':E'+str(start_row[1]), 'Response time (ms)', bold)
worksheet.merge_range('F'+str(start_row[1])+':H'+str(start_row[1]), 'Read MBps', bold)
worksheet.merge_range('I'+str(start_row[1])+':K'+str(start_row[1]), 'Write MBps', bold)
worksheet.merge_range('L'+str(start_row[1])+':N'+str(start_row[1]), 'Read IOPS', bold)
worksheet.merge_range('O'+str(start_row[1])+':Q'+str(start_row[1]), 'Write IOPS', bold)
worksheet.write_row('C'+str(start_row[1]+1),('max','mean','95th'))
worksheet.write_row('F'+str(start_row[1]+1),('max','mean','95th'))
worksheet.write_row('I'+str(start_row[1]+1),('max','mean','95th'))
worksheet.write_row('L'+str(start_row[1]+1),('max','mean','95th'))
worksheet.write_row('O'+str(start_row[1]+1),('max','mean','95th'))
# Heading cell merge for Disk part of dashboard

worksheet.merge_range('C'+str(start_row[2])+':E'+str(start_row[2]), 'Utilization (%)', bold)
worksheet.merge_range('F'+str(start_row[2])+':H'+str(start_row[2]), 'Response time (ms)', bold)
worksheet.write_row('C'+str(start_row[2]+1),('max','mean','95th'))
worksheet.write_row('F'+str(start_row[2]+1),('max','mean','95th'))


worksheet.write(0,0,"Onions VNX report " + serial, title) #rackspace report title
worksheet.write(1,0,"from " + str(df.poll_time.min()) + " to " + str(df.poll_time.max()) , bold)

worksheet.write(4,1,"Storage processors", bold) #dashboard description: Storage processors
worksheet.write(10,1,"10 most utilized LUNs", bold) #dashboard description: top 10 LUNs
worksheet.write(24,1,"Overutilized physical disks", bold) #dashboard description: top 10 LUNs

# END of dashboard generation

# Start of SPs part
sp_stats.to_excel(template_writer,'SPs',startrow=start_row[0] , startcol=0)
worksheet = template_writer.sheets['SPs']
worksheet.set_column(1, 1, 40)
rag_format(workbook,worksheet,'D'+str(start_row[0]+2)+':F'+str(start_row[0]+len(sp_stats.index)+1),50,70)

worksheet.merge_range('D'+str(start_row[0])+':F'+str(start_row[0]), 'Utilization (%)', bold)
worksheet.merge_range('G'+str(start_row[0])+':I'+str(start_row[0]), 'Response time (ms)', bold)
worksheet.merge_range('J'+str(start_row[0])+':L'+str(start_row[0]), 'Service time (ms)', bold)
worksheet.merge_range('M'+str(start_row[0])+':O'+str(start_row[0]), 'Queue length', bold)
worksheet.merge_range('P'+str(start_row[0])+':R'+str(start_row[0]), 'Read MBps', bold)
worksheet.merge_range('S'+str(start_row[0])+':U'+str(start_row[0]), 'Write MBps', bold)
worksheet.merge_range('V'+str(start_row[0])+':X'+str(start_row[0]), 'Read size (KB)', bold)
worksheet.merge_range('Y'+str(start_row[0])+':AA'+str(start_row[0]), 'Write size (KB)', bold)
worksheet.merge_range('AB'+str(start_row[0])+':AD'+str(start_row[0]), 'Read IOPS', bold)
worksheet.merge_range('AE'+str(start_row[0])+':AG'+str(start_row[0]), 'Write IOPS', bold)
worksheet.write_row('D'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('G'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('J'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('M'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('P'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('S'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('V'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('Y'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('AB'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('AE'+str(start_row[0]+1),('max','mean','95th'))


lun_stats.to_excel(template_writer,'LUNs',startrow=start_row[0] , startcol=0)
worksheet = template_writer.sheets['LUNs']
worksheet.set_column(1, 1, 40)
rag_format(workbook,worksheet,'G'+str(start_row[0]+2)+':I'+str(start_row[0]+len(lun_stats.index)+1),15,20)

worksheet.merge_range('D'+str(start_row[0])+':F'+str(start_row[0]), 'Utilization (%)', bold)
worksheet.merge_range('G'+str(start_row[0])+':I'+str(start_row[0]), 'Response time (ms)', bold)
worksheet.merge_range('J'+str(start_row[0])+':L'+str(start_row[0]), 'Service time (ms)', bold)
worksheet.merge_range('M'+str(start_row[0])+':O'+str(start_row[0]), 'Queue length', bold)
worksheet.merge_range('P'+str(start_row[0])+':R'+str(start_row[0]), 'Read MBps', bold)
worksheet.merge_range('S'+str(start_row[0])+':U'+str(start_row[0]), 'Write MBps', bold)
worksheet.merge_range('V'+str(start_row[0])+':X'+str(start_row[0]), 'Read size (KB)', bold)
worksheet.merge_range('Y'+str(start_row[0])+':AA'+str(start_row[0]), 'Write size (KB)', bold)
worksheet.merge_range('AB'+str(start_row[0])+':AD'+str(start_row[0]), 'Read IOPS', bold)
worksheet.merge_range('AE'+str(start_row[0])+':AG'+str(start_row[0]), 'Write IOPS', bold)
worksheet.write_row('D'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('G'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('J'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('M'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('P'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('S'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('V'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('Y'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('AB'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('AE'+str(start_row[0]+1),('max','mean','95th'))

disk_stats.to_excel(template_writer,'Disks',startrow=start_row[0] , startcol=0)
worksheet = template_writer.sheets['Disks']
worksheet.set_column(1, 1, 40)
rag_format(workbook,worksheet,'D'+str(start_row[0]+2)+':F'+str(start_row[0]+len(disk_stats.index)+1),50,70)

worksheet.merge_range('D'+str(start_row[0])+':F'+str(start_row[0]), 'Utilization (%)', bold)
worksheet.merge_range('G'+str(start_row[0])+':I'+str(start_row[0]), 'Response time (ms)', bold)
worksheet.merge_range('J'+str(start_row[0])+':L'+str(start_row[0]), 'Service time (ms)', bold)
worksheet.merge_range('M'+str(start_row[0])+':O'+str(start_row[0]), 'Queue length', bold)
worksheet.merge_range('P'+str(start_row[0])+':R'+str(start_row[0]), 'Read MBps', bold)
worksheet.merge_range('S'+str(start_row[0])+':U'+str(start_row[0]), 'Write MBps', bold)
worksheet.merge_range('V'+str(start_row[0])+':X'+str(start_row[0]), 'Read size (KB)', bold)
worksheet.merge_range('Y'+str(start_row[0])+':AA'+str(start_row[0]), 'Write size (KB)', bold)
worksheet.merge_range('AB'+str(start_row[0])+':AD'+str(start_row[0]), 'Read IOPS', bold)
worksheet.merge_range('AE'+str(start_row[0])+':AG'+str(start_row[0]), 'Write IOPS', bold)
worksheet.write_row('D'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('G'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('J'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('M'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('P'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('S'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('V'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('Y'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('AB'+str(start_row[0]+1),('max','mean','95th'))
worksheet.write_row('AE'+str(start_row[0]+1),('max','mean','95th'))

template_writer.save()

if args.nographs is True:
    print "No graphs option selected, exiting ..."
    end_time = time.strftime("%H:%M:%S")
    print 'time start: %s' % (start_time)
    print 'time end: %s' % (end_time)
    sys.exit()

#GRAPHING FUNCTIONS START HERE
obj_count = 0
for obj in reversed(sorted_obj_list):
    obj_count += 1
    print 'processing obj %s of %s' % (obj_count,len(sorted_obj_list))
    if obj_type(obj) == 'LUN':
        if not os.path.exists(obj.split('[')[0].strip().translate(None, '!@#$*')):
            os.makedirs(obj.split('[')[0].strip().translate(None, '!@#$*'))
        metric_df = df[df['object_name'] == obj][column_list]
        metric_df = metric_df.set_index(pd.DatetimeIndex(metric_df['poll_time']))
        for metric_set in metrics_set_list_SP:
            series_set_df = metric_df[metric_set]
            series_set_df.groupby(metric_df.index.hour).quantile(0.95).plot() #set graph granularity by pandas.groupby
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-typical_day95.png', dpi=args.dpi)
            plt.close()
            series_set_df.resample('1H').mean().plot()
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-hourly.png', dpi=args.dpi)
            plt.close()
            series_set_df.resample('1D').mean().plot()
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-daily.png', dpi=args.dpi)
            plt.close()
            series_set_df.resample('1W').mean().plot()
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-weekly.png', dpi=args.dpi)
            plt.close()
    elif obj_type(obj) == 'SP':
        if not os.path.exists(obj.split('[')[0].strip()):
            os.makedirs(obj.split('[')[0].strip())
        metric_df = df[df['object_name'] == obj][column_list]
        metric_df = metric_df.set_index(pd.DatetimeIndex(metric_df['poll_time']))
        print obj
        # print metric_df.groupby(metric_df.index.hour).quantile(0.95)
        print metric_df.groupby(metric_df.index.hour)
        for metric_set in metrics_set_list_SP:
            series_set_df = metric_df[metric_set]
            series_set_df.groupby(metric_df.index.hour).quantile(0.95).plot() #set graph granularity by pandas.groupby
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-typical_day95.png', dpi=args.dpi)
            plt.close()
            series_set_df.resample('1H').mean().plot()
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-hourly.png', dpi=args.dpi)
            plt.close()
            series_set_df.resample('1D').mean().plot()
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-daily.png', dpi=args.dpi)
            plt.close()
            series_set_df.resample('1W').mean().plot()
            plt.title(obj.split('[')[0] + ': ' + '_'.join(metric_set))
            plt.savefig(obj.split('[')[0].strip() + '/' + obj.split('[')[0] + '-' + '_'.join(metric_set) + '-weekly.png', dpi=args.dpi)
            plt.close()
    else:
        pass



end_time = time.strftime("%H:%M:%S")

print 'time start: %s' % (start_time)
print 'time end: %s' % (end_time)
