#!/usr/bin/python3
"""
This script is for updating an AMI in a safe and consistent way,
without requiring a reboot of the running server.

Feed it the aws profile name and a fqdn, and it will make a temporary copy of
the current AMI (with suffix '_TMP'), SSH to the running instance, stop
the specified service, freeze the one or more filesystems, initiate the creation of a new
AMI from the running instance, then unfreeze the filesystem(s) and restart the
service.  The AMI creation process should then continue for however
long it takes without affecting the running server. The service should
be unavailable for only a few seconds.
"""

import logging
from sys import argv
from os import environ, access, W_OK, chdir, path, getlogin
from string import ascii_uppercase, digits
from random import choice
from re import compile, search
from time import time, sleep
from boto3 import setup_default_session, resource
from boto3 import client as awsclient
from paramiko import SSHClient, SSHException
from awsretry import AWSRetry

# TODO: Get Paramiko logs (how?).
# TODO: Check fs is a mountpoint before freezing it.
# TODO: Support systemd as alternative.

class MyTimeoutError(Exception):
    pass

@AWSRetry.backoff()
def copy_ami(logger,
             source_image_id,
             new_image_name,
             dry_run=False,
             region='eu-west-1'):
    """
    Creates a copy of AMI source_image_id named new_image_name.
    """
    logger.info("Copying %s to %s.", source_image_id, new_image_name)
    token = ''.join(choice(ascii_uppercase + digits) for _ in range(16))
    response = awsclient('ec2').copy_image(
        SourceImageId=source_image_id,
        Name=new_image_name,
        SourceRegion=region,
        ClientToken=token,
        Encrypted=False,
        DryRun=dry_run)

    logger.info("Success: new AMI id will be %s.", response['ImageId'])


@AWSRetry.backoff()
def create_ami(logger,
               source_instance_id,
               new_image_name,
               no_reboot=True,
               dry_run=False):
    """
    Creates a new AMI named new_image_name based on the instance with id
    source_instance_id.
    """
    logger.info("Creating new AMI called %s from instance id %s.",
                new_image_name, source_instance_id)
    try:
        response = awsclient('ec2').create_image(
            Name=new_image_name,
            Description="Created from instance: {}".format(source_instance_id),
            InstanceId=source_instance_id,
            NoReboot=no_reboot,
            DryRun=dry_run)
    except Exception as e:
        # Log error, but continue, so as to unfreeze filesystems and restart service.
        logger.error('Failed to create AMI.')
        logger.debug(e)

    logger.info("Success: new AMI id will be %s.", response['ImageId'])


@AWSRetry.backoff()
def deregister_ami(logger, image_id, dry_run=False):
    """
    Deregisters the AMI with id image_id. If deregistration fails, aborts the
    program.
    """
    logger.info("Deregistering %s.", image_id)
    response = awsclient('ec2').deregister_image(
        ImageId=image_id, DryRun=dry_run)
    http_code = response['ResponseMetadata']['HTTPStatusCode']
    if http_code == 200:
        logger.info('Deregister request accepted.')
    else:
        logger.error("Failed to deregister AMI %s.", image_id)
        logger.debug("Failed with HTTP status code: %s.",
                     response['ResponseMetadata']['HTTPStatusCode'])
        raise SystemExit


@AWSRetry.backoff()
def find_ami_id(logger, search_name):
    """
    Returns the id string of the first AMI found owned by the current account
    and with an AMI Name matching search_name. If no matching AMI is found,
    aborts the program.
    """
    logger.info("Searching for AMI matching %s.", search_name)
    owner_id = awsclient('sts').get_caller_identity().get('Account')
    filters = [{'Name': 'owner-id', 'Values': [owner_id]}]
    images = resource('ec2').images.filter(Filters=filters).all()
    for image in images:
        if search_name == image.name:
            logger.info("Found %s with id %s.", image.name, image.id)
            return image.id

    logger.error("Could not find AMI %s.", search_name)
    raise SystemExit


@AWSRetry.backoff()
def find_instance_id(logger, search_name):
    """
    Returns the id string of the first instance found owned by the current
    account and with a Name tag matching search_name. If no matching instance
    is found, aborts the program.
    """
    logger.info("Searching for instance %s.", search_name)
    owner_id = awsclient('sts').get_caller_identity().get('Account')
    filters = [{'Name': 'owner-id', 'Values': [owner_id]}]
    instances = resource('ec2').instances.filter(Filters=filters).all()
    for instance in instances:
        if instance.tags:
            for tag in instance.tags:
                if tag['Key'] == 'Name':
                    name = tag['Value']
                    if name == search_name:
                        logger.info("Found %s with id %s.", search_name,
                                    instance.id)
                        return instance.id

    logger.error("Could not find an instance named %s.", search_name)
    raise SystemExit


def freeze_or_unfreeze_filesystems(logger, hostname, user, keyfile,
                                   filesystems, action):
    """
    SSH to hostname and freeze or unfreeze IO on each filesystem listed in
    filesystems. Note the difference between filesystems and mountpoints in this
    context. See https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=694624.
    Aborts the program if unsuccessful.
    """
    if action == 'freeze':
        verb = 'freezing'
    elif action == 'unfreeze':
        verb = 'unfreezing'

    logger.info("%s filesystem(s).", verb)

    sshclient = SSHClient()
    sshclient.load_system_host_keys()

    try:
        sshclient.connect(hostname, username=user, key_filename=keyfile)
    except SSHException as e:
        logger.error("SSH connection to %s failed.", hostname)
        logger.debug(e)
        raise SystemExit

    for fs in filesystems:
        ssh_command = "sudo fsfreeze --{} {}".format(action, fs)
        try:
            _, stdout, stderr = sshclient.exec_command(
                ssh_command, get_pty=True)
        except Exception as e:
            logger.error("Remote command %s failed.", ssh_command)
            logger.debug(e)
            raise SystemExit

        wait_seconds = 8
        endtime = time() + wait_seconds
        while not stdout.channel.eof_received:
            sleep(1)
            if time() > endtime:
                stdout.channel.close()
                stderr.channel.close()
                break

        stdout = stdout.readlines()
        stderr = stderr.readlines()

        if stderr:
            logger.info("%s failed.", ssh_command)
            logger.debug("%s stderr: %s.", ssh_command, stderr)
        else:
            logger.info("%s returned with no errors.", ssh_command)

        if stdout:
            logger.debug("%s stdout: %s.", ssh_command, stdout)

    sshclient.close()


def start_or_stop_service(logger, hostname, user, keyfile, action, service):
    """
    SSH to hostname and start or stop the service service. Aborts the program
    if unsuccessful.
    """
    if action == 'stop':
        verb = 'stopping'
    elif action == 'start':
        verb = 'starting'

    logger.info("%s %s service.", verb, service)
    
    sshclient = SSHClient()
    sshclient.load_system_host_keys()

    try:
        sshclient.connect(hostname, username=user, key_filename=keyfile)
    except SSHException as e:
        logger.error("SSH connection to %s failed.", hostname)
        logger.debug(e)
        raise SystemExit

    ssh_command = "sudo service {} {}".format(service, action)
    try:
        _, stdout, stderr = sshclient.exec_command(ssh_command, get_pty=True)
    except Exception as e:
        logger.error("Remote command %s failed.", ssh_command)
        logger.debug(e)
        raise SystemExit

    wait_seconds = 8
    endtime = time() + wait_seconds
    while not stdout.channel.eof_received:
        sleep(1)
        if time() > endtime:
            stdout.channel.close()
            stderr.channel.close()
            break

    stdout = stdout.readlines()
    stderr = stderr.readlines()

    if stderr:
        logger.info("%s failed.", ssh_command)
        logger.debug("%s stderr: %s.", ssh_command, stderr)
    else:
        logger.info("%s returned with no errors.", ssh_command)

    if stdout:
        logger.debug("%s stdout: %s.", ssh_command, stdout)

    sshclient.close()


def usage():
    print("{}: profile hostname".format(path.basename(__file__)))
    raise SystemExit


def validate_user(logger, user):
    real_user = getlogin()
    if real_user != user:
        logger.error("Script must be run as user %s.", user)
        raise SystemExit


@AWSRetry.backoff()
def wait_deregister(logger, image_name, **kwargs):
    """
    Repeatedly checks for the existence of AMI named image_name. If not found,
    returns success. If timeout is reached while an image with that name still
    exists, raises a MyTimeoutError exception.
    """
    timeout = int(kwargs.get('timeout', '1200'))
    runinterval = kwargs.get('runinterval', 8)

    logger.info("Waiting %s seconds for %s to deregister.", timeout, image_name)

    timer = 0
    while True:
        response = awsclient('ec2').describe_images(Filters=[{
            'Name': 'name',
            'Values': [image_name]
        }])
        if not response['Images']:
            logger.info("%s not found. Safe to continue.", image_name)
            return 0
        else:
            logger.info("%s still exists. Retrying...", image_name)
            sleep(runinterval)
            timer += runinterval
        if timer >= timeout:
            logger.error("Timeout value of %s seconds reached.", timeout)
            raise MyTimeoutError


def main(*args):
    """Main function."""

    if len(argv) is not 3:
        usage()

    aws_timeout = environ.get('TIMEOUT', '1200')
    loglevel = environ.get('LOGLEVEL', 'ERROR')
    unix_user = environ.get('USERNAME', 'root')
    key_file = environ.get('KEYFILE', '/root/.ssh/private_key')

    # AWS profile setup.
    profile = argv[1]
    setup_default_session(profile_name=profile)

    # Hostname of target server.
    fqdn = argv[2]
    regex = compile(r'(\w|-)+')
    hostname = search(regex, fqdn).group()

    # Filesystems to freeze on remote service.
    filesystems = ['/var/opt']

    # Logging.
    numeric_level = getattr(logging, loglevel)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {}".format(loglevel))

    script_name = path.basename(__file__)[:-3]

    logger = logging.getLogger(script_name)

    logfile = "/tmp/{}.log".format(script_name)
    if access("/var/log/{}.log".format(script_name), W_OK):
        logfile = "/var/log/{}.log".format(script_name)

    fh = logging.FileHandler(logfile)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)

    logger.setLevel(numeric_level)

    logger.info("Logger set to %s.", loglevel)
    logger.info("Running under profile %s.", profile)

    # Run the thing.
    current_image_id = find_ami_id(logger, "{}_CURRENT".format(hostname))
    copy_ami(logger, current_image_id, "{}_TMP".format(hostname))

    deregister_ami(logger, current_image_id)
    wait_deregister(
        logger,
        "{}_CURRENT".format(hostname),
        timeout=aws_timeout,
        runinterval=15)

    current_instance_id = find_instance_id(logger, hostname)
    validate_user(logger, unix_user)
    start_or_stop_service(logger, fqdn, unix_user, key_file, 'stop', 'postgresql')
    freeze_or_unfreeze_filesystems(logger, fqdn, unix_user, key_file,
                                   filesystems, 'freeze')

    create_ami(logger, current_instance_id, "{}_CURRENT".format(hostname))

    freeze_or_unfreeze_filesystems(logger, fqdn, unix_user, key_file,
                                   filesystems, 'unfreeze')
    start_or_stop_service(logger, fqdn, unix_user, key_file, 'start', 'postgresql')


if __name__ == '__main__':
    chdir(path.dirname(path.abspath(__file__)))
    main(argv[1:])

