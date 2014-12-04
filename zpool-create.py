#!/usr/bin/python

"""
zpool-create.py

Create storage pools.

Copyright (c) 2014  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""

import getopt
import sys
import simplejson
import signal
import subprocess
import re
import datetime


def usage():
    """
    Print usage.

    Inputs:
        None
    Outputs:
        None
    """
    cmd = sys.argv[0]

    print "%s [-h] [-c CONFIG]" % cmd
    print ""
    print "Storage Pool Creator"
    print ""
    print "Arguments:"
    print ""
    print "    -h, --help           print usage"
    print "    -c, --config         config file"


def logger(severity, message):
    """
    Log a message to stdout.

    Inputs:
        severity (str): Severity string
        message  (str): Log message
    Outputs:
        None
    """
    print " %s [%s] %s" % (str(datetime.datetime.now()), severity, message)


class Timeout(Exception):
    pass


class Execute(Exception):
    pass


def alarm_handler(signum, frame):
    raise Timeout


def execute(cmd, timeout=None):
    """
    Execute a command in the default shell. If a timeout is defined the command
    will be killed if the timeout is exceeded.

    Inputs:
        cmd     (str): Command to execute
        timeout (int): Command timeout in seconds
    Outputs:
        retcode  (int): Return code
        output  (list): STDOUT/STDERR
    """
    # Define the timeout signal
    if timeout:
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(timeout)

    try:
        # Execute the command and wait for the subprocess to terminate
        # STDERR is redirected to STDOUT
        phandle = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        # Read the stdout/sterr buffers and retcode
        stdout, stderr = phandle.communicate()
        retcode = phandle.returncode
    except Timeout, t:
        # Kill the running process
        phandle.kill()
        raise Timeout("command timeout of %ds exceeded" % timeout)
    except Exception, err:
        raise Execute(err)
    else:
        # Possible race condition where alarm isn't disabled in time
        signal.alarm(0)

    # stdout may be None and we need to acct for it
    if stdout and stdout is not None:
        output = stdout.strip()
    else:
        output = None

    return retcode, output


def execute_cmd(cmd, timeout=None):
    """
    Execute a command as defined in the config file and write it to the SIG
    document.

    Inputs:
        cmd     (str): Command to execute
        timeout (int): Command timeout in seconds
    Outputs:
        None
    """
    try:
        retcode, output = execute(cmd, timeout=timeout)
    except Exception, err:
        logger("ERROR", "command execution failed \"%s\"" % cmd)
        logger("ERROR", str(err))
        sys.exit(1)

    # Check the command return code
    if retcode:
        logger("ERROR", "command execution failed \"%s\"" % cmd)
        logger("ERROR", output)
        sys.exit(1)

    return output


def execute_nmc(cmd, timeout=None):
    """
    Execute an NMC command as defined in the config file and write it to the
    SIG document.

    Inputs:
        cmd     (str): NMC command to execute
        timeout (int): Command timeout in seconds
    Outputs:
        None
    """
    nmc = "nmc -c \"%s\"" % cmd
    try:
        retcode, output = execute(nmc, timeout=timeout)
    except Exception, err:
        logger("ERROR", "NMC command execution failed \"%s\"" % cmd)
        logger("ERROR", str(err))
        sys.exit(1)

    # Check the command return code
    if retcode:
        logger("ERROR", "NMC command execution failed \"%s\"" % cmd)
        logger("ERROR", output)
        sys.exit(1)

    return output


def get_slotmap():
    """
    Return parsed slotmap.

    Inputs:
        None
    Outputs:
        slotmap (dict): Parsed slotmap output
    """
    slotmap = {}

    cmd = "show lun slotmap"
    output = execute_nmc(cmd, timeout=600)

    for line in output.splitlines():
        if "Unmapped disks" in line:
            break
        elif re.search(r'(c[0-9]+t.*d[0-9]+\s)', line):
            lun, jbod, slot = line.split()[:-1]
            if jbod not in slotmap:
                slotmap[jbod] = {}
            slotmap[jbod][int(slot)] = lun

    return slotmap


def get_hddisco():
    """
    Return parsed hddisco output.

    Inputs:
        None
    Outputs:
        hddisco (dict): Parsed hddisco output
    """
    hddisco = {}

    # Execute hddisco command
    output = execute_cmd("hddisco", 300)

    # Iterate over each line of stdout
    for line in output.splitlines():
        path = 0
        # If the line begins with '='
        if line.startswith("="):
            current = line.lstrip('=').strip()
            hddisco[current] = {}
        # If the line begins with 'P'
        elif line.startswith('P'):
            continue
        else:
            k, v = [x.strip() for x in line.split(None, 1)]
            hddisco[current][k] = v

    return hddisco


def is_log(d, hddisco):
    """
    Determines if a disk is a valid log device. The determining factor is if
    the product string contains ZeusRAM.

    Inputs:
        d        (str): Device ID
        hddisco (dict): hddisco
    Outputs:
        log (bool): True/False
    """
    if hddisco[d]["product"] == "ZeusRAM":
        log = True
    else:
        log = False

    return log


def is_cache(d, hddisco):
    """
    Determines if a disk is a valid cache device. The determining factors are
    if the disk is an SSD and not a ZeusRAM device.

    Inputs:
        d        (str): Device ID
        hddisco (dict): hddisco
    Outputs:
        cache (bool): True/False
    """
    if hddisco[d]["is_ssd"] == "yes" and not is_log(d, hddisco):
        cache = True
    else:
        cache = False

    return cache


def prompt(question, answers):
    """
    Prompt the user with a question and only accept defined answers.

    Input:
        question (str): Question string
        answers (list): A list containing accpeted response value
    Output:
        answer (str|int): Provided answer
    """
    print question

    # Print a numbered list of answers
    for i in range(len(answers)):
        print ' %d. %s' % (i+1, answers[i])

    while True:
        choice = raw_input(">>> ")
        try:
            answer = answers[int(choice)-1]
        # A ValueError is raised when a string is cast to an int
        except ValueError:
            print "Invalid input."
        # An IndexError is raise when the list index is out of range
        except IndexError:
            print "Invalid input."
        else:
            break

    return answer


def prompt_yn(question):
    """
    Prompt the user with a yes or no question.

    Input:
        question (str): Question string
    Output:
        answer (bool): Answer True/False
    """
    while True:
        choice = raw_input("%s [y|n] " % question)
        if choice == "y":
            answer = True
            break
        elif choice == "n":
            answer = False
            break
        else:
            print "Invalid input."

    return answer


def build_log(log_config, slotmap, hddisco):
    """
    Build the list of log devid's.

    Inputs:
        log_config (list): JSON log config
    Outputs:
        log (list): log devid's
    """
    log = []
    for l in log_config:
        disks = [slotmap[j][s] for j, s in l]
        for d in disks:
            if not is_log(d, hddisco):
                logger("ERROR", "Invalid log device %s %s %s " %
                       (d, hddisco[d]["vendor"], hddisco[d]["product"]))
                sys.exit(1)
        log.append(disks)

    return log


def build_cache(cache_config, slotmap, hddisco):
    """
    Build the list of cache devid's.

    Inputs:
        cache_config (list): JSON cache config
    Outputs:
        cache (list): cache devid's
    """
    cache = [slotmap[j][s] for j, s in cache_config]
    for c in cache:
        if not is_cache(c, hddisco):
            logger("ERROR", "Invalid cache device %s %s %s " %
                   (c, hddisco[c]["vendor"], hddisco[c]["product"]))
            sys.exit(1)

    return cache


def build_vdev(vdev_config, slotmap, hddisco):
    """
    Build the list of vdev devid's.

    Inputs:
        vdev_config (list): JSON vdev config
    Outputs:
        vdev (list): vdev devid's
    """
    vdev = []
    for v in vdev_config:
        disks = [slotmap[j][s] for j, s in v]
        for d in disks:
            if is_log(d, hddisco) or is_cache(d, hddisco):
                logger("ERROR", "Invalid vdev device %s %s %s " %
                       (d, hddisco[d]["vendor"], hddisco[d]["product"]))
                sys.exit(1)
        vdev.append(disks)

    return vdev


def zpool_create(name, redundancy, vdev, cache, log):
    """
    Create a zpool.

    Inputs:
        name       (str): Pool name
        redundancy (str): Redundancy type, i.e. raidz2, mirror, etc
        vdev      (list): A list containing each logical grouping of vdevs as
                          lists
        cache     (list): A list of cache devices which will be striped
        log       (list): A list containing each logical grouping of log
                          devices which will be mirrored
    Outputs:
        None
    """
    mnt = "/volumes/%s" % name
    cmd = ("zpool create -f -m %s -o failmode=continue -o autoreplace=on -O "
           "compression=lz4 %s" % (mnt, name))

    # Build device strings and append to the create command
    vdev_str = " ".join([" ".join([redundancy, " ".join(v)]) for v in vdev])
    cmd = " ".join([cmd, vdev_str])

    # Append log devices if they are defined
    if log is not None:
        log_str = " ".join([" ".join(["mirror", " ".join(l)]) for l in log])
        cmd = " ".join([cmd, "log", log_str])

    # Append cache devices if they are defined
    if cache is not None:
        cache_str = " ".join(cache)
        cmd = " ".join([cmd, "cache", cache_str])

    logger("INFO", "Creating pool %s" % name)
    execute_cmd(cmd)


def main():
    # Parse command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], ":hc:", ["help", "config="])
    except getopt.GetoptError as err:
        logger("ERROR", str(err))
        usage()
        sys.exit(1)

    # Initialize required arguments
    config = "layouts.json"

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--config"):
            config = a

    # Open the configuration file
    logger("INFO", "Opening configuration file")
    try:
        fh = open(config)
    except Exception, err:
        logger("ERROR", "Cannot open the config file")
        logger("ERROR", str(err))
        sys.exit(1)

    # Parse the configuration file
    logger("INFO", "Parsing configuration file")
    try:
        layouts = simplejson.load(fh, encoding=None, cls=None,
                                  object_hook=None)
    except Exception, err:
        logger("ERROR", "Cannot parse the config file")
        logger("ERROR", str(err))
        sys.exit(1)
    finally:
        fh.close()

    # Prompt for configuration
    layout = prompt("Please select the layout type.", layouts.keys())
    if not prompt_yn("Continue with '%s' layout?" % layout):
        logger("INFO", "Exiting")
        sys.exit()

    # Scan JBOD's
    execute_nmc("setup jbod rescan")

    # Get LUN slotmap to build pool with
    slotmap = get_slotmap()

    # Get hddisco to verify drive type
    hddisco = get_hddisco()

    # Build device lists
    for pool in layouts[layout]:
        try:
            name = pool["name"]
            redundancy = pool["redundancy"]
        except Exception, err:
            logger("ERROR", "Invalid configuration file")
            logger("ERROR", str(err))

        logger("INFO", "Building %s device list" % name)

        # Build devid lists
        try:
            if "log" in pool:
                log = build_log(pool["log"], slotmap, hddisco)
            else:
                log = None
            if "cache" in pool:
                cache = build_cache(pool["cache"], slotmap, hddisco)
            else:
                cache = None
            vdev = build_vdev(pool["vdev"], slotmap, hddisco)
        except Exception, err:
            logger("ERROR", str(err))
            logger("ERROR", "Invalid configuration file and/or slot placement")
            logger("ERROR", "Please review the configuration file AND slotmap")
            sys.exit(1)

        zpool_create(name, redundancy, vdev, cache, log)

    logger("INFO", "Complete!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger("ERROR", "Killed by user")
