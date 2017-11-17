#!/usr/bin/python3

from boto3 import setup_default_session, client, resource
from botocore import exceptions
from time import sleep
from optparse import OptionParser
from os import path, access, W_OK
import logging

# TODO: options.ignore_string should be a list of strings.
# TODO: make logfile location settable.

#============================
# Functions
#============================

def test_protected(snapshot, string):
    logger.debug("Function test_protected received parameters: snapshot: {} and string: {}.".format(snapshot.id, string))
    if snapshot.tags:
        logger.debug("Snapshot {} has tags.".format(snapshot.id))
        for tag in snapshot.tags:
            if tag['Key'] == 'Name':
                logger.debug("Snapshot {} has a Name tag.".format(snapshot.id))
                name = tag['Value']
                if string in name:
                    logger.info("Snapshot {} has the protected string {} within its Name tag and therefore will not be deleted.".format(snapshot.id, string))
                    return(True)
                else:
                    logger.debug("Protected string {} was not found in snapshot {}'s Name tag.".format(string, snapshot.id))
                    return(False)
    else:
        logger.debug("Snapshot {} has no tags.".format(snapshot.id))
        return(False)

def test_ownership(snapshot):
    logger.debug("Function test_ownership received parameter: snapshot: {}.".format(snapshot.id))
    current_identity = client('sts').get_caller_identity().get('Account')
    logger.debug("Current account id is {}.".format(current_identity))
    logger.debug("Comparing snapshot owner id: {} and current account id: {}.".format(snapshot.owner_id, current_identity))
    if snapshot.owner_id == current_identity:
        logger.debug("Snapshot is owned by the current account.")
        return(True)
    else:
        logger.debug("Snapshot is not owned by this account and is therefore not deleteable at the moment.")
        return(False)

def delete_snapshots(ignore_string, wait_time, max_results):
    logger.debug("Function delete_snapshots received parameters: ignore_string: {}, wait_time: {} and max_results: {}.".format(ignore_string, wait_time, max_results))
    ec2c = client('ec2')
    ec2r = resource('ec2')
    list_snapshots = []
    deleted_snapshots = []
    retained_snapshots = []

    response = ec2c.describe_snapshots(MaxResults=5,RestorableByUserIds=['self'])
    for snapshot in response['Snapshots']:
        logger.debug("Adding snapshot {} to list.".format(snapshot['SnapshotId']))
        list_snapshots.append(snapshot)
    while 'NextToken' in response:
        token = response['NextToken']
        response = ec2c.describe_snapshots(
            MaxResults=max_results,
            NextToken=token,
            RestorableByUserIds=['self']
        )
        for snapshot in response['Snapshots']:
            logger.debug("Adding snapshot {} to list.".format(snapshot['SnapshotId']))
            list_snapshots.append(snapshot)

        for snapshot in list_snapshots:
            s = ec2r.Snapshot(snapshot['SnapshotId'])
            logger.debug("Processing snapshot {}.".format(s))
            logger.debug("Testing if snapshot is protected or not owned by this account.")
            test_protected_result = test_protected(s, ignore_string)
            test_ownership_result = test_ownership(s)
            if (test_protected_result == True or test_ownership_result == False):
                logger.info("Snapshot {} added to list of snapshots to retain.".format(s.id))
                retained_snapshots.append(s.id)
                continue
            else:
                try:
                    logger.debug("Attempting to delete snapshot {}.".format(s))
                    s.delete(DryRun=False)
                    logger.info("Snapshot {} deleted.".format(s.snapshot_id))
                    deleted_snapshots.append(s.id)
                except exceptions.ClientError as e:
                    if e.response['Error'].get('Code') == "InvalidSnapshot.InUse":
                        retained_snapshots.append(s.id)
                        logger.debug("Caught exception: ClientError while attempting to delete a snapshot.")
                        logger.info("Snapshot {} is in use, so intentionally not deleted.".format(s.snapshot_id))
                    else:
                        raise SystemExit(e)
        del list_snapshots[:]
        sleep(wait_time)
    return(deleted_snapshots, retained_snapshots)

def deregister_images(tmp_string):
    logger.debug("Function deregister_images received parameters: tmp_string: {}.".format(tmp_string))
    ec2c = client('ec2')
    ec2r = resource('ec2')
    stsc = client('sts')
    images_to_deregister = []
    images_deregistered = []
    images_retained = []

    owner_id = stsc.get_caller_identity().get('Account')
    filters = [
            {'Name': 'owner-id',
            'Values': [owner_id]}
    ]
    images = ec2r.images.filter(Filters=filters).all()

    for image in images:
        if tmp_string in image.name:
            images_to_deregister.append(image)
            logger.info("Added image {} to deregister list.".format(image.id))
        else:
            images_retained.append(image.id)
            logger.debug("Image will be ignored.".format(image.id))
    for image in images_to_deregister:
        response = ec2c.deregister_image(
            ImageId="{}".format(image.id),
            DryRun=False
        )
        http_code = response['ResponseMetadata']['HTTPStatusCode']
        if http_code == 200:
            images_deregistered.append(image.id)
            logger.info("Image {} deregistered.".format(image.id))
    return(images_deregistered, images_retained)

#============================
# Main
#============================

#== Options =======
parser = OptionParser()
parser.add_option("--accounts", dest="accounts",
    action="store", default="dev",
    type="string", help="A list of comma-separated profile names (usually synonymous with accounts) in which to operate, eg. 'dev,stg'.")

parser.add_option("--log-level", dest="loglevel",
    action="store", default="WARNING",
    type="string", help="DEBUG|INFO|WARNING|ERROR|CRITICAL")

parser.add_option("--ignore-string", dest="ignore_string",
    action="store", default="_BACKUP",
    type="string", help="Snapshots with this substring anywhere in their name will not be deleted (all other snapshots currently not in use will be). Default is 'BACKUP'.")

parser.add_option("--tmp-string", dest="tmp_string",
    action="store", default="_TMP",
    type="string", help="Images with this suffix (and only these images) will be deregistered by the script. Default is '_TMP'.")

parser.add_option("--wait-time", dest="wait_time",
    action="store", default=10,
    type="int", help="Time to wait between polling AWS. Use alongside --max-results to tweak performance.")

parser.add_option("--max-results", dest="max_results",
    action="store", default=5,
    type="int", help="The number of results to process each wait_time.")
(options, args) = parser.parse_args()


#== Logging =======
numeric_level = getattr(logging, options.loglevel.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError("Invalid log level: {}".format(options.loglevel))

script_name = path.basename(__file__)
logger = logging.getLogger(script_name)

logfile = "/tmp/{}.log".format(script_name)
if access("/var/log/{}.log".format(script_name), W_OK):
    logfile = "/var/log/{}.log".format(script_name)

handler = logging.FileHandler(logfile)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(numeric_level)


#== Run the thing =
for account in options.accounts.split(","):
    setup_default_session(profile_name=account)
    logger.info("Starting operation in account {}.".format(account))
    del_result = delete_snapshots(options.ignore_string, options.wait_time, options.max_results)
    logger.info("Deleted snapshots: {}.".format(del_result[0]))
    logger.info("Retained snapshots: {}.".format(del_result[1]))
    dereg_result = deregister_images(options.tmp_string)
    logger.info("Deregistered images: {}.".format(dereg_result[0]))
    logger.info("Retained images: {}.".format(dereg_result[1]))
    logger.info("Ending operation in account {}.".format(account))
