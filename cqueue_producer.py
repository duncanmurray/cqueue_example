#!/usr/bin/env python2.7

##############################################################################
#                                                                            #          
#               A simple example of Rackspace Cloud Queues                   #
#                        Author: Duncan Murray 2013                          #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License");            #
# you may not use this file except in compliance with the License.           #
# You may obtain a copy of the License at                                    #
#                                                                            #
#     http://www.apache.org/licenses/LICENSE-2.0                             #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
#                                                                            # 
##############################################################################      

import os
import pyrax
import uuid
import logging
import argparse
from pyrax import exceptions as pex

# Set default location of pyrax configuration file
CREDFILE = "~/.rackspace_cloud_credentials"

def main():

    # Define what a valid TTL for a message is
    def valid_ttl(sec):
        sec = int(sec)
        if not 60 <= sec <= 1209600:
            raise argparse.ArgumentTypeError("Message TTL must be between 60 and 1209600 seconds")
        return sec

    # Read in argumants fron command line to over ride defaults
    parser = argparse.ArgumentParser(description=("A Simple cloud queues example"))
    parser.add_argument("-q", "--queue", action="store", required=True,
                        metavar="QUEUE", type=str,
                        help=("The name of your message queue"))
    parser.add_argument("-m", "--message", action="store", required=True,
                        metavar="MESSAGE", type=str,
                        help=("The message that you want to put in your queue")) 
    parser.add_argument("-c", "--credfile", action="store", required=False,
                        metavar="CREDENTIALS_FILE", type=str,
                        help=("The location of your pyrax configuration file"),
                        default=CREDFILE)
    parser.add_argument("-r", "--region", action="store", required=False,
                        metavar="REGION", type=str,
                        help=("Region where your lsyncd configuration group is (defaults"
                              " to 'LON') [ORD, DFW, LON, SYD, IAD]"), 
                        choices=["ORD", "DFW", "LON", "SYD", "IAD", "HKG"],
                        default="LON")
    parser.add_argument("-t", "--ttl", action="store", required=False,
                        metavar="TTL", type=valid_ttl,
                        help=("The time to live for the message.(defaults to 60 seconds, Must "
                            "be between 60 and 1209600 seconds)"),
                        default=60)
    parser.add_argument("-v", "--verbose", action="store_true", required=False,
                        help=("Turn on debug verbosity"),
                        default=False)

    # Parse arguments (validate user input)
    args = parser.parse_args()
    # Configure log formatting
    logFormatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    rootLogger = logging.getLogger()
    # Check what level we should log with
    if args.verbose:
        rootLogger.setLevel(logging.DEBUG)
    else:
        rootLogger.setLevel(logging.WARNING)
    # Configure logging to console
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)   

    # Define the authentication credentials file location and request that
    # pyrax makes use of it. If not found, let the client/user know about it.

    # Use a credential file in the following format:
    # [rackspace_cloud]
    # username = myusername
    # api_key = 01234567890abcdef
    # region = LON

    # Set identity type as rackspace
    pyrax.set_setting("identity_type", "rackspace")

    # Test that the pyrax configuration file provided exists
    try:
        creds_file = os.path.expanduser(args.credfile)
        pyrax.set_credential_file(creds_file, args.region)
    # Exit if authentication fails
    except pex.AuthenticationFailed:
        rootLogger.critical("Authentication failed")
        rootLogger.info("%s", """Please check and confirm that the API username, 
                                     key, and region are in place and correct."""
                       )
        exit(1)
    # Exit if file does not exist
    except pex.FileNotFound:
        rootLogger.critical("Credentials file '%s' not found" % (creds_file))
        rootLogger.info("%s", """Use a credential file in the following format:\n
                                 [rackspace_cloud]
                                 username = myuseername
                                 api_key = 01sdf444g3ffgskskeoek0349"""
                       )
        exit(2)

    # Make it easy to reference cloud queues
    pq = pyrax.queues

    # For now lets just generate a uuid but this shold be constant
    my_client_id = str(uuid.uuid4())
    pq.client_id = my_client_id

    # Check that the request queue exists
    exists = pq.queue_exists(args.queue)

    if exists:
        rootLogger.info("Queue matching '%s' found in '%s'" % (args.queue, args.region))
    else:
        rootLogger.critical("No queues named '%s' found in '%s'" % (args.queue, args.region))
        exit(3)

    # Post the message to the queue
    try:
        msg = pq.post_message(args.queue, args.message, args.ttl)
        rootLogger.info("Successfully posted message to '%s''" % (args.queue))
    except pex.ServiceNotAvailable:
        rootLogger.critical("Unable to post message to '%s'" % (args.queue))

# Run the main program
if __name__ == '__main__':
    main()
