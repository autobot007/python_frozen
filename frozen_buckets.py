import os
import datetime

# Path where frozen indexes are
nfs_path = '/var/splunk_storage/nfs/indexes'

# log file where script will output to
output_log_path = '/root/output_cold/frozen.log'

# open a file handle to write into it
log = open(output_log_path, 'w')


# function to convert UTC timestamp
def convert_utc(utc_time):
    return datetime.datetime.fromtimestamp(int(utc_time)).strftime('%Y-%m-%d %H:%M:%S.%s')

# loop through each directory (indexes) in nfs path
for item in os.listdir(nfs_path):
    if os.path.isdir(nfs_path + '/' + item):
        for direct in os.listdir(nfs_path + '/' + item):
            # Move forward only if direct name starts with db or rb
            if direct.startswith(('db', 'rb')):
                # get the file size in bytes and convert it to KB
                dir_size = os.popen("du -sk " + nfs_path + "/" + item + "/" + direct + " | awk {'print $1'}").read()
                parsed_file_name = direct.split('_')
                #print(parsed_file_name) # ['rb or db', 'oldest_time', 'newest_time', 'local_id', 'guid' ]
                # write parsed file name content to log file
                log.write('name_of_index:' + item +
                          ' bucket_id:' + nfs_path + '/' + item + '/' + direct +
                          ' newest_time:' + convert_utc(parsed_file_name[1]) +
                          ' oldest_time:' + convert_utc(parsed_file_name[2]) +
                          ' localid:' + parsed_file_name[3] +
                          ' guid:' + parsed_file_name[4] +
                          ' file_size_kb:' + str(dir_size))

# close the file handle
log.close()


