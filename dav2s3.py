#!/usr/bin/python
"""This module provides the dav2s3 class and offers CLI argument parsing for using it out of the box."""
from __future__ import print_function
from os import listdir, mkdir, remove, removedirs
from os.path import isfile, join, exists, dirname
from datetime import datetime
import shutil
import string
import argparse
import ConfigParser
import requests
import easywebdav
import boto3

class Dav2S3(object):
    """Get a bunch of files from Demandware logs directory via WebDAV and put them into a specified S3 bucket.

    Trough parametrization allow to:
    - only upload local files to S3 without touching WebDAV
    - delete all files on WebDAV after download and upload to S3
    - select files by size, file name fragment, last modify date or file extension
    Account details for WebDAV must be present in config file.
    There must be a preconfigured boto3 installation for S3 to work.
    """

    def __init__(self, config_file, verbose, no_confirm):
        """Read configuration values from config file and connect to WebDAV."""
        super(Dav2S3, self).__init__()
        # read credentials from config
        configuration = self.read_config_file(config_file)
        if configuration:
            self.dav_cred = configuration['webdav']
            self.s3_conf = configuration['s3']
            self.base_path_dw_logs = configuration['paths']['base_path_dw_logs']
            self.verbose = verbose
            self.no_confirm = no_confirm
            self.allowed_extfrag_chars = string.ascii_letters + string.digits + '_-'
        else:
            print("Error while reading configuration. Aborted.")

        self.webdav = self.connect_webdav()
        self.path = dirname(__file__)

    def read_config_file(self, config_file):
        """Read the config file."""
        config = ConfigParser.ConfigParser()
        read_file_result = config.read(config_file)
        rf_result_length = len(read_file_result)
        if read_file_result and rf_result_length > 0:
            configuration = self.create_section_dict(config)
            return configuration
        elif rf_result_length <= 0:
            print("Error: Cannot read file "+str(config_file)+". Is it there?")
            return False
        else:
            print("Generic error during reading config file.")
            return False

    @classmethod
    def create_section_dict(cls, config):
        """Create a dict of each section from the config file."""
        section_dict = {}
        sections = config.sections()
        for section in sections:
            section_dict[section] = {}
            options = config.options(section)
            for option in options:
                section_dict[section][option] = config.get(section, option)
        return section_dict

    @classmethod
    def prompt_user(cls, message):
        """Show a message and await user confirmation or cancel, return according boolean."""
        yes = set(['yes', 'y', 'ye', 'yup', 'yo', 'sure'])
        no = set(['no', 'n', 'nope', 'na', 'better not'])
        print(message)
        while True:
            choice = raw_input().lower()
            if choice in yes:
                return True
            elif choice in no:
                return False
            else:
                print("Please respond with 'yes' or 'no'")

    def connect_webdav(self):
        """Connect to a WebDAV host."""
        try:
            webdav = easywebdav.connect(
                self.dav_cred['url'],
                username=self.dav_cred['user'],
                password=self.dav_cred['password'],
                protocol=self.dav_cred['protocol'])
            return webdav
        except Exception as err:
            print("Error during WebDAV connection: "+str(err))
            return False

    def folder_empty(self, folder):
        """Check if a WebDAV folder contains files."""
        if len(folder) >= 2:
            return False
        elif len(folder) == 1:
            return True
        else:
            print("Undefined error while checking for empty folder.")

    @classmethod
    def get_file_name(cls, path):
        """Extract file name from partial file path."""
        name = path[path.rfind('/')+1:]
        return name

    def verbose_print(self, message):
        """Print message, if verbose-flag is set."""
        if self.verbose:
            print(message)

    def download(self, source_folder, search_type, search_value):
        """Download files from WebDAV source folder to local temporary folder before uploading to S3 target folder."""
        err_already_exists = 0
        if not exists('temp'):
            mkdir('temp')
        if not source_folder is None:
            full_source_path = self.base_path_dw_logs+"/"+source_folder
        else:
            self.verbose_print("No source folder given, assuming root as source.")
            full_source_path = self.base_path_dw_logs
        try:
            result = self.webdav.ls(full_source_path)
        except Exception as err:
            if type(err) is requests.exceptions.SSLError:
                print("The set protocol '"+self.dav_cred['protocol']+"' is not valid for this instance. Original error: "+str(err))
            else:
                print(err.message)
            exit()
        
        if not self.folder_empty(result):
            for i in range(1, len(result)):
                filename = self.get_file_name(result[i].name)
                if filename:
                    if not self.check_file_temp(filename):
                        self.verbose_print("Downloading "+filename)
                        self.webdav.download(result[i].name, 'temp/'+filename)
                    else:
                        self.verbose_print(filename+" already exists. Skipping...")
                        err_already_exists += 1
        else:
            self.verbose_print("Source folder is empty, aborting.")
            exit()
        if err_already_exists >= 1:
            plural = ''
            if err_already_exists > 1:
                plural = 's'
            self.verbose_print(err_already_exists+" file"+plural+" already exists in local temp folder. Please check to avoid data loss. No files will be deleted from WebDAV.")
            exit()

    @classmethod
    def check_file_temp(cls, filename):
        """Check if a file with given filename exists in local 'temp' folder."""
        files = [f for f in listdir('temp') if isfile(join('temp', f))]
        if files != None:
            return bool(filename in files)
        else:
            return False

    def delete(self, source_folder):
        """Delete all files in WebDAV source folder."""
        full_source_path = self.base_path_dw_logs+"/"+source_folder
        result = self.webdav.ls(full_source_path)
        instance_url = self.dav_cred['url']
        if self.no_confirm:
            print("---> Attention!")
            user_confirmation = self.prompt_user("All "+str(len(result)-1)+" files in the folder "+instance_url+full_source_path+" will be deleted. Please check again and confirm! (yes|no)")
        else:
            user_confirmation = True

        if user_confirmation:
            if not self.folder_empty(result):
                for i in range(1, len(result)):
                    filename = self.get_file_name(result[i].name)
                    if filename:
                        if self.check_file_temp(filename):
                            self.webdav.delete(join(full_source_path, filename))
        else:
            self.verbose_print("Aborted.")
            exit()

    def cleanup(self):
        """Delete all files in local 'temp' folder if they are existing in S3 bucket."""
        try:
            local_files = [f for f in listdir('temp') if isfile(join('temp', f))]
            bucket = self.get_bucket()
            if bucket:
                logs = bucket.objects.all()
                for log in logs:
                    for _file in local_files:
                        if _file == log.key:
                            self.verbose_print(_file+" was uploaded. Removing temporary copy.")
                            remove(join('temp', _file))
                self.handle_residual_files()
            else:
                self.verbose_print('No connection to S3 was possible. To avoid possible data loss no local files have been deleted.')
        except OSError, e:
            if e.errno == 2:
                print("Error: 'temp' folder not found and could not be created. Please report this bug!")
        except Exception, e:
            print("Error during cleanup: "+str(e))

    def handle_residual_files(self):
        """Handle files that are left over, not existing on S3 but still in temp folder."""
        try:
            removedirs('temp')
        except Exception, e:
            # print(e.errno) # Use this to determine the error the OS gives and catch it
            if e.errno == 66:
                if not exists('uncertain'):
                    mkdir('uncertain')
                residual_files = [f for f in listdir('temp') if isfile(join('temp', f))]
                for _file in residual_files:
                    shutil.move(join('temp', _file), 'uncertain')
                self.verbose_print("At least one of the local files is not existing in S3 bucket. The files in question have been moved to a directory 'uncertain', please review.")
            else:
                print("Error during handling of residual files: "+str(e))

    def upload(self, source_path, target_path=''):
        """Upload all files from given path to given bucket."""
        source_path = join(self.path, source_path)
        if target_path.find('/') == 0:
            target_path = target_path[1:]
        if target_path.find('/') == -1 or target_path.rfind('/') == len(target_path)-1:
            target_path = target_path+'/'
        bucket = self.get_bucket()
        if bucket:
            files = [f for f in listdir(source_path) if isfile(join(source_path, f))]
            for _file in files:
                self.verbose_print("Uploading: "+str(target_path)+str(_file))
                try:
                    data = open(source_path+'/'+str(_file), 'rb')
                    bucket.put_object(Key=str(target_path)+str(_file), Body=data)
                except Exception, e:
                    print("Error during upload of "+str(target_path)+str(_file)+": "+str(e))
                    return False
            return True

    def get_bucket(self):
        """Create S3 bucket connection and connect to given bucket from config file."""
        s3 = boto3.resource('s3')
        bucket_name = str(self.s3_conf['bucket'])
        for external_bucket in s3.buckets.all():
            if external_bucket.name == bucket_name:
                bucket = s3.Bucket(bucket_name)
                return bucket
        print("Bucket does not exist. Did you miss-spell it?")

    def validate_search_value(self, value, supposed_type):
        """Validate a given search value against its supposed type."""
        if supposed_type == 'ext' or supposed_type == 'frag':
            value = str(value).replace('"', '')
            if supposed_type == 'ext':
                value.replace('.', '')
            for char in value:
                if char not in self.allowed_extfrag_chars:
                    print("File extension contains illegal characters. It can only contain: "+self.allowed_extfrag_chars)
                    return False
            return True
        if supposed_type == 'date-from':
            try:
                datetime.strptime(str(value), '%m-%d-%Y')
            except ValueError:
                print("The given date seems not to be correct. Date format must be MM-dd-YYYY.")
                return False
            return True
        if supposed_type == 'size':
            for char in value:
                if char not in string.digits:
                    print("Filesize value illegal. It can only contain numbers.")
                    return False
            return True

# handle arguments
parser = argparse.ArgumentParser(description='Get a bunch of files from Demandware logs directory via WebDAV, put them into a specified S3 bucket and delete them from WebDAV afterwards. Offers options for delete-only or upload-only.')
parser.add_argument('-c',
                    metavar='path/to/config_file.cfg',
                    dest='config_file',
                    default='default.cfg',
                    help='Specifies a configuration file to use. Use relative or absolute path including filename. If not specified, it will look for "default.cfg".'
                   )
parser.add_argument('-s',
                    metavar='remote/source/folder',
                    dest='source_folder',
                    help='Specifies the source folder in the "Logs" folder on WebDAV. If omitted, root directory level is assumed. See "base_path_dw_logs" entry in configuration file to change the remote root directory path.'
                   )
parser.add_argument('-t',
                    metavar='remote/target/folder',
                    dest='target_folder',
                    help='Specifies the target folder in the S3 bucket. If omitted, all files are placed on root level.'
                   )
parser.add_argument('-u',
                    metavar='local/source/folder',
                    dest='upload',
                    help='If this flag is set, do upload only. Uploads all files from a given local path to the given S3 bucket. If this is set, no WebDAV operations will commence.'
                   )
parser.add_argument('-d',
                    dest='delete',
                    action='store_true',
                    help='If this flag is set, delete all files in the given source WebDAV folder, after they are downloaded and uploaded. If there was a problem during upload, they will still be deleted remotely but kept in the local "temp" folder. If Dav2S3 detects one or more local files have not been uploaded to S3 but are scheduled to be cleaned up, it will stop and give notice of this. Using the -u flag, the user can then attempt to only upload the local files again. NOTE: This option will show a interactive security confirmation before deleting files. If you want to automate this, use -n flag to supress this behaviour.'
                   )
parser.add_argument('-v',
                    dest='verbose',
                    action='store_true',
                    help='Starts with verbose mode enabled. Note that a interactive security confirmation before deleting will be shown even without this flag. To supress it, use the -n flag.'
                   )
parser.add_argument('-n',
                    dest='no_confirmation',
                    action='store_false',
                    help='If this flag is set while -d is active, no confirmation of the user will be necessary for deleting files. Use this only if you are certain you know what you do and need the program to run without user interaction.'
                   )
parser.add_argument('-k',
                    dest='search_type',
                    choices=['ext', 'date-from', 'size', 'frag'],
                    help='Set the search mask with which files on WebDAV are chosen for download. So this is, what type of value you are sorting your files. If this flag is set, you need to set -f also, to specifiy the actual value of the search.'
                   )
parser.add_argument('-f',
                    dest='search_value',
                    help='Based on the chosen search parameter. So this is the value that is used to actually filter the files, it must match the type given in -k flag. If -k is not set an error is given and the program exits. The formats are: \
                    ext (file extension) - string, characters and numbers only. \
                    date-from (files that where last modified on given date) - date string with format MM-dd-YYYY. \
                    size (file size) - integer. \
                    frag (file name fragment) - substring of the filename, all matches that contain the complete substring are considered hits.'
                   )

# parse arguments
args = parser.parse_args()

# instantiate
tool_instance = Dav2S3(config_file=args.config_file, verbose=args.verbose, no_confirm=args.no_confirmation)

# check if -k is set, if yes, -f must be set also. Otherwise just return error and exit
if args.search_type:
    if not args.search_value:
        print('You set -k but did not give a search value via -f flag. You must set both flags or none of them. Aborting.')
        exit()
# Same goes for -f flag
if args.search_value:
    if not args.search_type:
        print('You set -f but did not set a search type via -k flag. You must set both flags or none of them. Aborting.')

# print(tool_instance.validate_search_value(args.search_value, args.search_type))

# main program run
# if optional upload flag given, only upload and exit
if args.upload:
    try:
        tool_instance.verbose_print("Upload flag detected")
        tool_instance.upload(args.upload, args.target_folder)
    except Exception, e:
        print('Something went wrong: '+str(e))
    exit()

# normal operation
# Download files from WebDAV
tool_instance.download(args.source_folder, args.search_type, search_value=None)
try:
    tool_instance.upload('temp', args.target_folder) # Upload to WebDAV
except Exception, e:
    print('Something went wrong: '+str(e))
    exit()

# if optional delete flag given, delete files from dav afterwards
if args.delete:
    tool_instance.delete(args.source_folder)

# Clean temp file if all files are uploaded successfully
tool_instance.cleanup()
exit()
