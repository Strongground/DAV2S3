# DAV2S3 (WebDAV to AWS S3 Bucket Command Line Tool) #

## Installation ##

DAV2S3 requires Python 2.7 and `pip`.

Requirements are found in `requirements` file.

Use `pip install -r requirements` to install them.

## Usage ##

### Before first run ###

Put `dav2s3.py` in any folder. dav2s3 needs read/write access and will create a `temp` folder when first executed. dav2s3 will store all log files in `temp` after download and prior to upload. It does clean up after upload ist finished and verified, but it will need enough free space to store them in the meantime.

Copy `default.cfg` and change the contents, including path to Commerce Cloud instance, username and password.

Remember to setup boto3 AWS plugin.

### Execute ###

Run in the same folder via `python dav2s3.py`.

Run `python dav2s3 -help` or `python dav2s3 -h` to get detailed instruction on how to use and which parameters are available. If running for the first time or unsure, always use `-v` flag, as it enables warnings and explicit user confirmation before deleting anything.