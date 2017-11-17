from os import environ, walk, path, unlink, rmdir, getpid
from string import ascii_uppercase, digits
from random import choice
from time import sleep
from tempfile import mkdtemp
from fabric.api import task, abort
from fabric.colors import red, green, yellow
from botocore import exceptions
from boto3 import setup_default_session, client, resource
from awsretry import AWSRetry

# TODO: check for presence of BUILD_VERSION_NAME and exit if not set.

#===============================================================================
# Classes.
#===============================================================================
class MyTimeoutError(Exception):
    pass


#===============================================================================
# Functions.
#===============================================================================


def clean_up(cookie):
    """
    Removes any temporary files with names containing cookie as a substring,
    then removes any parent directory they had.
    """
    print(yellow("Removing temporary files and directory."))
    files_to_delete = set()
    dirs_to_delete = set()
    for root, _, files in walk('/tmp'):  # Hardcoded for safety.
        for f in files:
            if cookie in f:
                files_to_delete.add(path.join(root, f))
    for ftd in files_to_delete:
        dirs_to_delete.add(path.dirname(ftd))
    for d_file in files_to_delete:
        unlink(d_file)
    for d_dir in dirs_to_delete:
        rmdir(d_dir)
    print(green("Clean up complete."))


def copy_ami(source_image_id, new_image_name, dry_run=False):
    """
    Creates a copy of AMI source_image_id named new_image_name.
    Returns the id string of the new image.
    """
    region = environ['REGION']
    print(yellow("Copying {}.".format(source_image_id)))
    token = ''.join(choice(ascii_uppercase + digits) for _ in range(16))
    ec2 = client('ec2')
    ec2.meta.events._unique_id_handlers['retry-config-ec2'][
        'handler']._checker.__dict__['_max_attempts'] = 20
    response = ec2.copy_image(
        SourceImageId=source_image_id,
        Name=new_image_name,
        SourceRegion=region,
        ClientToken=token,
        Encrypted=False,
        DryRun=dry_run)
    print(green("Success: new AMI id will be {}.".format(response['ImageId'])))
    return response['ImageId']


def create_ami(source_instance_id, new_image_name, dry_run=False):
    """
    Creates a new AMI named new_image_name based on the instance with id
    source_instance_id. Returns the new AMI id string.
    """
    print(yellow("Creating new AMI {} from instance {}.".format(
        new_image_name, source_instance_id)))
    ec2 = client('ec2')
    ec2.meta.events._unique_id_handlers['retry-config-ec2'][
        'handler']._checker.__dict__['_max_attempts'] = 20
    response = ec2.create_image(
        Name=new_image_name,
        Description="Created from instance: {}".format(source_instance_id),
        InstanceId=source_instance_id,
        NoReboot=True,
        DryRun=dry_run)
    print(green("Success: new AMI id will be {}.".format(response['ImageId'])))
    return response['ImageId']


def deregister_ami(image_id, dry_run=False):
    """
    Deregisters the AMI with id image_id. If deregistration fails, aborts the
    program.
    """
    print(yellow("Deregistering {}.".format(image_id)))
    ec2 = client('ec2')
    ec2.meta.events._unique_id_handlers['retry-config-ec2'][
        'handler']._checker.__dict__['_max_attempts'] = 20
    response = ec2.deregister_image(ImageId=image_id, DryRun=dry_run)
    http_code = response['ResponseMetadata']['HTTPStatusCode']
    if http_code == 200:
        print(green('Deregister request accepted.'))
    else:
        print(red("Failed with HTTP status code: {}.".format(
            response['ResponseMetadata']['HTTPStatusCode'])))
        abort("Failed to deregister AMI {}.".format(image_id))


@AWSRetry.backoff()
def deregister_from_elbs(instance_id, elb_names):
    """
    Deregisters instance_id from each load balancer (classic type) in collection
    elb_names.
    """
    print(yellow("Deregistering {} from classic ELBs {}.".format(
        instance_id, elb_names)))
    for elb in elb_names:
        client('elb').deregister_instances_from_load_balancer(
            LoadBalancerName=elb, Instances=[{
                'InstanceId': instance_id
            }])
        print(green("Deregistered from {}.".format(elb)))


@AWSRetry.backoff()
def deregister_from_target_groups(instance_id, target_group_arns):
    """
    Deregisters instance_id from each target group in collection
    target_group_arns.
    """
    print(yellow("Deregistering {} from target groups {}.".format(
        instance_id, target_group_arns)))
    for tg in target_group_arns:
        client('elbv2').deregister_targets(
            TargetGroupArn=tg, Targets=[{
                'Id': instance_id
            }])
        print(green("Deregistered from {}.".format(tg)))


@AWSRetry.backoff()
def find_ami_id(search_name):
    """
    Returns the id string of the first AMI found owned by the current account
    and with an AMI Name matching search_name. If no matching AMI is found,
    aborts the program.
    """
    print(yellow("Searching for AMI matching {}.".format(search_name)))
    owner_id = client('sts').get_caller_identity().get('Account')
    filters = [{'Name': 'owner-id', 'Values': [owner_id]}]
    images = resource('ec2').images.filter(Filters=filters).all()
    for image in images:
        if search_name == image.name:
            print(green("Found {} with id {}.".format(image.name, image.id)))
            return image.id
    abort("Could not find AMI {}".format(search_name))


@AWSRetry.backoff()
def find_elbs(instance_id):
    """
    Returns a set containing the names of each load balancer (classic type)
    which currently has instance_id as one of it's instances. If none are found,
    returns None.
    """
    print(yellow("Searching for classic ELBs associated with instance {}.".
                 format(instance_id)))
    elb_names = set()
    response = client('elb').describe_load_balancers()
    for elb in response['LoadBalancerDescriptions']:
        for instance in elb['Instances']:
            if instance['InstanceId'] == instance_id:
                print(green("Found ELB named {}.".format(
                    elb['LoadBalancerName'])))
                elb_names.add(elb['LoadBalancerName'])
    if elb_names:
        return elb_names
    else:
        print(green(
            "Could not find any classic ELBs associated with instance {}.".
            format(instance_id)))


@AWSRetry.backoff()
def find_elbv2s(instance_id):
    """
    Returns a set containing the names of each load balancer (v2 type) which
    currently has instance_id within one of it's associated target groups. If
    none are found, returns None.
    """
    print(yellow("Searching for v2 ELBs associated with instance {}.".format(
        instance_id)))
    v2 = client('elbv2')
    elb_names = set()
    r1 = v2.describe_load_balancers()
    for lb in r1['LoadBalancers']:
        r2 = v2.describe_target_groups(LoadBalancerArn=lb['LoadBalancerArn'])
        for tg in r2['TargetGroups']:
            r3 = v2.describe_target_health(TargetGroupArn=tg['TargetGroupArn'])
            for thd in r3['TargetHealthDescriptions']:
                if thd['Target']['Id'] == instance_id:
                    print(green("Found ELB named {}.".format(
                        lb['LoadBalancerName'])))
                    elb_names.add(lb['LoadBalancerName'])
    if elb_names:
        return elb_names
    else:
        print(green("Could not find any v2 ELBs associated with instance {}.".
                    format(instance_id)))


def find_file(name, startdir):
    """
    Returns an absolute path filename string if name is found anywhere under
    startdir; otherwise returns None.
    """
    print(yellow("Searching for {} under {}.".format(name, startdir)))
    for root, _, files in walk(startdir):
        if name in files:
            abs_path = path.join(root, name)
            print(green("Found {}.".format(abs_path)))
            return abs_path


@AWSRetry.backoff()
def find_instance_id(search_name):
    """
    Returns the id string of the first running instance found owned by the
    current account and with a Name tag matching search_name. If no matching
    instance is found, aborts the program.
    """
    print(yellow("Searching for {}.".format(search_name)))
    owner_id = client('sts').get_caller_identity().get('Account')
    filters = [{'Name': 'owner-id', 'Values': [owner_id]}]
    instances = resource('ec2').instances.filter(Filters=filters).all()
    candidates = []
    for instance in instances:
        if instance.tags:
            for tag in instance.tags:
                if tag['Key'] == 'Name':
                    name = tag['Value']
                    if name == search_name:
                        print(green("Found instance {} with id {}.".format(
                            search_name, instance.id)))
                        candidates.append(instance)

    for instance in candidates:
        if instance.state['Name'] == 'running':
            print(green("{} is currently running, so selecting it.".format(
                instance.id)))
            return instance.id
        else:
            print(yellow("{} is currently {}, so not selecting it.".format(
                instance.id, instance.state['Name'])))

    abort("Could not find a running instance named {}".format(search_name))


@AWSRetry.backoff()
def find_target_groups(instance_id):
    """
    Returns a set containing the arns of each target group which currently
    contains instance_id as one of it's instances. If none are found, returns
    None.
    """
    print(yellow("Searching for target groups associated with instance {}.".
                 format(instance_id)))
    elbv2 = client('elbv2')
    tg_arns = set()
    r1 = elbv2.describe_target_groups()
    for tg in r1['TargetGroups']:
        r2 = elbv2.describe_target_health(TargetGroupArn=tg['TargetGroupArn'])
        for thd in r2['TargetHealthDescriptions']:
            if thd['Target']['Id'] == instance_id:
                print(green("Found target group named {}.".format(
                    tg['TargetGroupName'])))
                tg_arns.add(tg['TargetGroupArn'])
    if tg_arns:
        return tg_arns
    else:
        print(green(
            "Could not find any target groups associated with instance {}.".
            format(instance_id)))


@AWSRetry.backoff()
def register_with_elbs(instance_id, elb_names):
    """
    Registers instance_id with each load balancer (classic type) listed in
    collection elb_names.
    """
    print(yellow("Registering {} with classic ELBs {}.".format(
        instance_id, elb_names)))
    for elb in elb_names:
        client('elb').register_instances_with_load_balancer(
            LoadBalancerName=elb, Instances=[{
                'InstanceId': instance_id
            }])
        print(green("Registered with {}.".format(elb)))


@AWSRetry.backoff()
def register_with_target_groups(instance_id, target_group_arns):
    """
    Registers instance_id with each target group listed in collection
    target_group_arns.
    """
    print(yellow("Registering {} with target groups: {}.".format(
        instance_id, target_group_arns)))
    for tg in target_group_arns:
        client('elbv2').register_targets(
            TargetGroupArn=tg, Targets=[{
                'Id': instance_id
            }])
        print(green("Registered with {}.".format(tg)))


def retrieve_from_disk(category, cookie):
    """
    Returns a list with each item containing one line of the first file found
    named cookie_category under '/tmp' (hardcoded for safety). If no file can be
    found with this name, returns None.
    """
    print(yellow("Attempting to retrieve stored {}s.".format(category)))
    full_path = find_file("{}_{}".format(cookie, category), '/tmp')
    if full_path:
        print(green("Found file at {}.".format(full_path)))
        ids = []
        try:
            with open(full_path, 'r') as fh:
                ids = fh.readlines()
        except Exception as e:
            abort("{} exists, but the contents were not retrieved. Error: {}.".
                  format(full_path, e))
        print(green("Found {}.".format(ids)))
        if ids:
            ids = [i.strip() for i in ids]
            return ids
    else:
        print(green("Could not find any associated {} resources.".format(
            category)))


def store_to_disk(category, ids, cookie):
    """
    Creates a temporary directory and file with unique names (the file name is
    made up of category and cookie, the directory name is random), and writes
    the contents of collection ids to the file, one entry per line.
    """
    ids = [item.rstrip('\r,\n') for item in ids]
    print(yellow(
        "Attempting to store {}s {} to disk for use during postdeploy.".format(
            category, ids)))
    tmpdir = mkdtemp()
    filename = "{}_{}".format(cookie, category)
    full_path = path.join(tmpdir, filename)
    try:
        with open(full_path, 'w') as tmp:
            for i in ids:
                tmp.write("{}\n".format(i))
    except Exception as e:
        abort("Error: {}.".format(e))
    print(green("Stored {} to disk at {}.".format(ids, full_path)))


def wait_copy(image_id, runinterval=8):
    """
    Repeatedly checks for the existence of image_id and for it's availability.
    If image_id is not found or does not become available before timeout is
    reached, aborts the program.
    """
    timeout = int(environ['TIMEOUT'])
    print(yellow("Waiting for {} to complete.".format(image_id)))
    timer = 0
    while True:
        ec2 = client('ec2')
        ec2.meta.events._unique_id_handlers['retry-config-ec2'][
            'handler']._checker.__dict__['_max_attempts'] = 20
        response = ec2.describe_images(Filters=[{
            'Name': 'image-id',
            'Values': [image_id]
        }])
        if len(response['Images']) == 0:
            print(yellow("{} not found. Retrying..".format(image_id)))
            sleep(runinterval)
            timer += runinterval
        elif len(response['Images']) == 1:
            print(green("{} found.".format(image_id)))
            state = response['Images'][0]['State']
            if state != 'available':
                print(yellow("{} not currently available. Retrying..".format(
                    image_id)))
                sleep(runinterval)
                timer += runinterval
            elif state == 'available':
                print(green("{} is currently available.".format(image_id)))
                return 0
        if timer >= timeout:
            print(
                yellow("Timeout value of {} seconds reached".format(timeout)))
            abort("wait_copy() timed out.")


def wait_deregister(image_name, runinterval=8):
    """
    Repeatedly checks for the existence of AMI named image_name. If not found,
    returns success. If TIMEOUT is reached while an image with that name still
    exists, raises a MyTimeOut exception.
    """
    timeout = int(environ['TIMEOUT'])
    print(yellow("Waiting for {} to deregister.".format(image_name)))
    timer = 0
    while True:
        ec2 = client('ec2')
        ec2.meta.events._unique_id_handlers['retry-config-ec2'][
            'handler']._checker.__dict__['_max_attempts'] = 20
        response = ec2.describe_images(Filters=[{
            'Name': 'name',
            'Values': [image_name]
        }])
        if len(response['Images']) == 0:
            print(green("{} not found. Safe to continue.".format(image_name)))
            return 0
        else:
            print(yellow("{} still exists. Retrying..".format(image_name)))
            sleep(runinterval)
            timer += runinterval
        if timer >= timeout:
            print(
                yellow("Timeout value of {} seconds reached".format(timeout)))
            raise MyTimeoutError


#===============================================================================
# Main.
#===============================================================================


@task
def main(operation, fqn):
    """
    Usage: 'fab main:<operation>,<instance name>'. Valid operations are
    'predeploy' and 'postdeploy'.

    Continuity between pre- and post- operations is provided by files written
    under /tmp using the Jenkins-inserted BUILD_VERSION_NAME envvar as a cookie.
    """
    # Set default values.
    environ['TIMEOUT'] = environ.get('TIMEOUT', '1200')
    environ['REGION'] = environ.get('REGION', 'eu-west-1')

    # Set up boto3 session. A profile is required as an envvar.
    setup_default_session(profile_name=environ['PROFILE'])
    print(yellow("Running under profile {}.".format(environ['PROFILE'])))

    if operation == 'predeploy':
        # Determine which load balancers instance is currently in.
        current_instance_id = find_instance_id(fqn)
        current_elb_names = find_elbs(current_instance_id)
        current_target_group_arns = find_target_groups(current_instance_id)

        # Write any results to disk (so they can be re-registered during
        # postdeploy).
        # Uses BUILD_VERSION_NAME to provide continuity between predeploy and
        # postdeploy operations.
        if current_elb_names:
            store_to_disk("Load_Balancer", current_elb_names,
                          environ['BUILD_VERSION_NAME'])
        if current_target_group_arns:
            store_to_disk("Target_Group", current_target_group_arns,
                          environ['BUILD_VERSION_NAME'])

        # Remove target instance from ELBs.
        if current_elb_names:
            deregister_from_elbs(current_instance_id, current_elb_names)
        # Remove target instance from target groups.
        if current_target_group_arns:
            deregister_from_target_groups(current_instance_id,
                                          current_target_group_arns)

        # Make temporary backup of current AMI.
        current_image_id = find_ami_id("{}_CURRENT".format(fqn))
        copy_ami(current_image_id, "{}_TMP".format(fqn))

        # Discard current AMI (to make room for new one).
        deregister_ami(current_image_id)
        wait_deregister("{}_CURRENT".format(fqn))

    if operation == 'postdeploy':
        # Retrieve load balancer information.
        current_instance_id = find_instance_id(fqn)
        elb_names = retrieve_from_disk("Load_Balancer",
                                       environ['BUILD_VERSION_NAME'])
        target_group_arns = retrieve_from_disk("Target_Group",
                                               environ['BUILD_VERSION_NAME'])
        # Re-register instance with Load Balancers.
        if elb_names:
            register_with_elbs(current_instance_id, elb_names)
        if target_group_arns:
            register_with_target_groups(current_instance_id, target_group_arns)

        # Delete temporary files and directories.
        clean_up(environ['BUILD_VERSION_NAME'])

        # Make new AMI from updated instance.
        create_ami(current_instance_id, "{}_CURRENT".format(fqn))


@task
def test():
    """
    Unit testing.
    """

    # Comment out any tests that are not required.
    tests = [
        'find_ami_id', 'find_instance_id', 'find_file', 'find_elbv2s',
        'find_elbs', 'find_target_groups', 'copy_ami', 'create_ami',
        'store_to_disk', 'wait_copy', 'wait_deregister', 'deregister_ami',
        'deregister_from_elbs', 'deregister_from_target_groups',
        'retrieve_from_disk', 'register_with_elbs',
        'register_with_target_groups', 'clean_up'
    ]

    # Set default values.
    environ['TIMEOUT'] = '3'
    environ['REGION'] = environ.get('REGION', 'eu-west-1')

    # Set up boto3 session.
    # mgmt account is used for all unit tests as instances are stable (can be
    # overridden with envvar).
    if environ.get('PROFILE') is not None:
        p = environ('PROFILE')
    else:
        p = "mgmt"
    setup_default_session(profile_name=p)
    print(yellow("Running under profile {}.".format(p)))

    # Values to use in tests.
    test_instance_name = "servername"
    test_instance_id = "i-deeeeeadbeeef"
    test_image_name = "servername_CURRENT"
    test_image_id = "ami-deadbeef"

    # Carry out the tests.
    if 'find_ami_id' in tests:
        print(yellow("Testing find_ami_id."))
        try:
            find_ami_id(test_image_name)
        except Exception as e:
            abort("Error: find_ami_id test failed: {}.".format(e))
        print(green("find_ami_id test passed."))

    if 'find_instance_id' in tests:
        print(yellow("Testing find_instance_id."))
        try:
            find_instance_id(test_instance_name)
        except Exception as e:
            abort("Error: find_instance_id test failed: {}.".format(e))
        print(green("find_instance_id test passed."))

    if 'find_file' in tests:
        print(yellow("Testing find_file."))
        result = find_file("hosts", "/etc")
        if result == "/etc/hosts":
            print(green("find_file test passed."))
        else:
            abort("Error: find_file test failed.")

    if 'find_elbv2s' in tests:
        print(yellow("Testing find_elbv2s."))
        try:
            result = find_elbv2s(test_instance_id)
        except Exception as e:
            abort("Error: find_elbv2s test failed: {}.".format(e))
        if result:
            print(green("find_elbv2s test passed."))
        else:
            abort("Error: find_elbv2s test failed.")

    if 'find_elbs' in tests:
        print(yellow("Testing find_elbs."))
        try:
            result = find_elbs(test_instance_id)
        except Exception as e:
            abort("Error: find_elbs test failed: {}.".format(e))
        if result:
            print(green("find_elbs test passed."))
        else:
            abort("Error: find_elbs test failed.")

    if 'find_target_groups' in tests:
        print(yellow("Testing find_target_groups."))
        try:
            result = find_target_groups(test_instance_id)
        except Exception as e:
            abort("Error: find_target_groups test failed: {}.".format(e))
        if result:
            print(green("find_target_groups test passed."))
        else:
            abort("Error: find_target_groups test failed.")

    if 'copy_ami' in tests:
        print(yellow("Testing copy_ami."))
        try:
            copy_ami(test_image_id, "foobar", dry_run=True)
        except exceptions.ClientError:
            print(green("copy_ami test passed. Note that this this is only "
                        "a dry-run test."))
        except Exception as e:
            abort("Error: copy_ami test failed: {}.".format(e))

    if 'create_ami' in tests:
        print(yellow("Testing create_ami."))
        try:
            create_ami(test_image_id, "foobar", dry_run=True)
        except exceptions.ClientError:
            print(green("create_ami test passed. Note that this is only a "
                        "dry-run test."))
        except Exception as e:
            abort("Error: create_ami test failed: {}.".format(e))

    if 'store_to_disk' in tests:
        print(yellow("Testing store_to_disk."))
        lines = ["love\n", "falafel\n"]
        pid = getpid()
        try:
            store_to_disk("unit_test", lines, pid)
        except Exception as e:
            abort("Error: store_to_disk test failed: {}.".format(e))

    if 'wait_copy' in tests:
        print(yellow("Testing wait_copy."))
        try:
            wait_copy(test_image_id, runinterval=1)
        except Exception as e:
            abort("Error: wait_copy test failed: {}.".format(e))
        print(green("wait_copy test passed. Note that this is a basic test "
                    "which does not rely on an actual image being copied."))

    if 'wait_deregister' in tests:
        print(yellow("Testing wait_deregister."))
        try:
            wait_deregister(
                "{}_CURRENT".format(test_instance_name), runinterval=1)
        except MyTimeoutError:
            print(green("wait_deregister test passed (test relies on the "
                        "function timing out). Note however that this is a "
                        "basic test which does not rely on an actual "
                        "image deregistration."))
        except Exception as e:
            abort("Error: wait_deregister test failed: {}.".format(e))

    if 'deregister_ami' in tests:
        print(yellow("Testing deregister_ami."))
        try:
            deregister_ami(test_image_id, dry_run=True)
        except exceptions.ClientError:
            print(green("deregister_ami test passed. Note that this is only "
                        "a dry-run test."))
        except Exception as e:
            abort("Error: deregister_ami test failed: {}.".format(e))

    if 'deregister_from_elbs' in tests:
        print(yellow("Test for deregister_from_elbs is not currently "
                     "implemented as there is no dry-run facility available. "
                     "Hopefully this will change in future."))

    if 'deregister_from_target_groups' in tests:
        print(yellow("Test for deregister_from_target_groups is not currently "
                     "implemented as there is no dry-run facility available. "
                     "Hopefully this will change in future."))

    if 'retrieve_from_disk' in tests:
        print(yellow("Testing retrieve_from_disk."))
        if 'store_to_disk' not in tests:
            print(red("Cannot run this test unless store_to_disk is also in "
                      "the test suite."))
        else:
            try:
                a, b = retrieve_from_disk("unit_test", pid)
            except Exception as e:
                abort("Error: retrieve_from_disk test failed: {}.".format(e))
            if a == "love" and b == "falafel":
                print(green("Test retrieve_from_disk passed."))
            else:
                print("retrieve_from_disk test failed due to unexpected "
                      "results being returned.")

    if 'register_with_elbs' in tests:
        print(
            yellow("Test for register_with_elbs is not currently implemented "
                   "as there is no dry-run facility available. Hopefully "
                   "this will change in future."))

    if 'register_with_target_groups' in tests:
        print(yellow("Test for register_with_target_groups is not currently "
                     "implemented as there is no dry-run facility available. "
                     "Hopefully this will change in future."))

    if 'clean_up' in tests:
        print(yellow("Testing clean_up"))
        try:
            clean_up(str(pid))
        except Exception as e:
            abort("Error: clean_up test failed: {}.".format(e))

    # Finish up.
    print(green("Testing complete."))
