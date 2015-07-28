#!/usr/bin/env python3
#
# Copyright (C) 2015 Olaf Lessenich
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License v2 as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA 021110-1307, USA.

import argparse
import multiprocessing
from multiprocessing import Process, Value, Lock

import mailbox
import hashlib


class Counter(object):
    def __init__(self, del_messages=0, del_bytes=0):
        self.del_messages = Value('i', del_messages)
        self.del_bytes = Value('i', del_bytes)
        self.lock = Lock()


    def add_deleted(self, del_messages, del_bytes):
        with self.lock:
            self.del_messages.value += del_messages
            self.del_bytes.value += del_bytes


    def get_deleted_messages(self):
        with self.lock:
            return self.del_messages.value


    def get_deleted_bytes(self):
        with self.lock:
            return self.del_bytes.value


KBFACTOR = float(1 << 10)

fast = False
verbose = False


def print_usage(path):
    print("Usage: %s [MAILDIR]" % path)


def hash_content(message):
    if message.is_multipart():
        hashes = []
        for payload in message.get_payload():
            hashes.append(hash_content(payload))

        return hashlib.sha256(hashes[0].join("").encode()).hexdigest()
    else:
        content = str(message.get_payload()).encode()
        return hashlib.sha256(content).hexdigest()


def remove(mbox, to_remove, counter, dry_run=False):

    mbox.lock()
    try:
        for key, hashsum in to_remove.items():
            message = mbox.get(key)
            action = "deleting"
            info = ""
            if dry_run:
                action = "would delete"
            if verbose:
                info = " (%s bytes, sha256: %s)" % (message['Content-Length'], hashsum)
            print("  %s %s: \"%s\"%s" % (action, message['Message-Id'], message['Subject'], info))
            deleted_bytes = 0
            if message['Content-Length'] is not None:
                deleted_bytes = int(message['Content-Length'])

            counter.add_deleted(1, deleted_bytes)
            if not dry_run:
                mbox.remove(key)
    finally:
        mbox.flush()
        mbox.close()


def prune(mbox, counter, dry_run=False):
    messages = {}
    to_remove = {}

    for key, message in mbox.iteritems():
        message_id = message['Message-Id']
        if message_id is None:
            continue

        try:
            if message_id in messages:
                    messages[message_id].append(key)
            else:
                    messages[message_id] = [key]
        except TypeError:
            pass

    # Check duplicate message ids as a first heuristic
    # this is reasonably fast
    for message_id in messages:
        if len(messages[message_id]) > 1:
            dupes = {}
            for key in messages[message_id]:
                message = mbox.get(key)
                hashsum = "-"
                if not fast:
                    hashsum = hash_content(message)
                if hashsum in dupes:
                    dupes[hashsum].append(key)
                else:
                    dupes[hashsum] = [key]

            # Check duplicate hashes to be safe
            # this should be reasonably precise
            # Please note that if fast is True, there were no real hashes computed
            for hashsum in dupes:
                if len(dupes[hashsum]) > 1:
                    for key in dupes[hashsum][1:]:
                        to_remove[key] = hashsum

    remove(mbox, to_remove, counter, dry_run)

    for subdir in mbox.list_folders():
        print("Subdir found: %s", subdir)
        prune(subdir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fast", help="use fast heuristic based on message IDs (unsafe)", action="store_true")
    parser.add_argument("-n", "--dry-run", help="perform a trial run with no changes made", action="store_true")
    parser.add_argument("-v", "--verbose", help="show verbose output", action="store_true")
    parser.add_argument("target_dirs", default=[], nargs="+")
    args = parser.parse_args()

    verbose = args.verbose
    fast = args.fast

    counter = Counter()
    num_cores = multiprocessing.cpu_count()
    procs = [Process(target=prune, args=(mailbox.Maildir(target_dir), counter, args.dry_run)) for target_dir in args.target_dirs]

    # split processes into chunks depending on number of cores available
    for i in range(0, len(procs), 2*num_cores):
        for p in procs[i:i+2*num_cores]: p.start()
        for p in procs[i:i+2*num_cores]: p.join()

    print()
    print("Deleted %d messages (%dK)." % (counter.get_deleted_messages(), int(counter.get_deleted_bytes()/KBFACTOR)))
