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
import os
from multiprocessing import Process, Value, Lock, Queue
from queue import Empty
from time import perf_counter
from enum import Enum

import mailbox
import hashlib


class Mode(Enum):
    CHECK = 1
    PRUNE = 2


class Counter(object):
    def __init__(
        self, lock, del_messages=0, del_bytes=0, messages=0, mboxes=0
    ):
        self.lock = lock
        self.del_messages = Value("i", del_messages)
        self.del_bytes = Value("i", del_bytes)
        self.messages = Value("i", messages)
        self.mboxes = Value("i", mboxes)

    def add_deleted(self, del_messages, del_bytes):
        with self.lock:
            self.del_messages.value += del_messages
            self.del_bytes.value += del_bytes

    def add_messages(self, messages):
        with self.lock:
            self.messages.value += messages

    def add_mboxes(self, mboxes):
        with self.lock:
            self.mboxes.value += mboxes

    def get_deleted_messages(self):
        with self.lock:
            return self.del_messages.value

    def get_deleted_bytes(self):
        with self.lock:
            return self.del_bytes.value

    def get_messages(self):
        with self.lock:
            return self.messages.value

    def get_mboxes(self):
        with self.lock:
            return self.mboxes.value


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
                info = " (%s bytes, sha256: %s)" % (
                    message["Content-Length"],
                    hashsum,
                )
            print(
                '  %s %s: "%s"%s'
                % (action, message["Message-Id"], message["Subject"], info)
            )
            deleted_bytes = 0
            if message["Content-Length"] is not None:
                deleted_bytes = int(message["Content-Length"])

            counter.add_deleted(1, deleted_bytes)

            if not dry_run:
                mbox.remove(key)
    finally:
        mbox.flush()
        mbox.unlock()
        mbox.close()


def prune(mbox, counter, dry_run=False):
    messages = {}
    to_remove = {}

    counter.add_mboxes(1)

    for key, message in mbox.iteritems():
        message_id = message["Message-Id"]
        if message_id is None:
            continue

        try:
            if message_id in messages:
                messages[message_id].append(key)
            else:
                messages[message_id] = [key]
        except TypeError:
            pass

    counter.add_messages(len(messages))

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
            #
            # Please note that if fast is True,
            # there were no real hashes computed
            for hashsum in dupes:
                if len(dupes[hashsum]) > 1:
                    for key in dupes[hashsum][1:]:
                        to_remove[key] = hashsum

    remove(mbox, to_remove, counter, dry_run)

    for subdir in mbox.list_folders():
        print("Subdir found: %s", subdir)
        prune(subdir, counter, dry_run)


def validate(mbox, path, sep=";"):
    mandatory_headers = ["date", "from"]
    common_headers = ["message-id", "subject"]

    for key, message in mbox.iteritems():
        for header in mandatory_headers:
            if header not in message:
                print(
                    "Error%sno %s%s%s"
                    % (sep, header, sep, os.path.join(path, mbox._toc[key]))
                )

        for header in common_headers:
            if header not in message:
                print(
                    "Warning%sno %s%s%s"
                    % (sep, header, sep, os.path.join(path, mbox._toc[key]))
                )

        for defect in message.defects:
            print(
                "ParseWarning%s%s%s%s"
                % (
                    sep,
                    type(defect).__name__,
                    sep,
                    os.path.join(path, mbox._toc[key]),
                )
            )

    for subdir in mbox.list_folders():
        print("Subdir found: %s", subdir)
        validate(subdir, path + os.sep + subdir, sep)


def process(queue, counter, mode, dry_run=False, sep=";"):
    if verbose:
        print("Started process %d" % os.getpid())

    while not queue.empty():
        try:
            target_dir = queue.get(False)
            required_dirs = ["cur", "new", "tmp"]
            is_valid_maildir = True

            for subdir in required_dirs:
                if not os.path.exists(os.path.join(target_dir, subdir)):
                    is_valid_maildir = False
                    break

            if not is_valid_maildir:
                print("Skipping invalid Maildir '%s'" % target_dir)
                continue

            if mode == Mode.CHECK:
                validate(mailbox.Maildir(target_dir), target_dir, sep)
            elif mode == Mode.PRUNE:
                prune(mailbox.Maildir(target_dir), counter, dry_run)

        except Empty:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--check",
        help="check for invalid mails, " + "does not delete anything",
        action="store_true",
    )
    parser.add_argument(
        "-f",
        "--fast",
        help="use fast heuristic based on message IDs " + "(unsafe)",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        help="perform a trial run with no changes made",
        action="store_true",
    )
    parser.add_argument(
        "-v", "--verbose", help="show verbose output", action="store_true"
    )
    parser.add_argument("target_dirs", default=[], nargs="+")
    args = parser.parse_args()

    verbose = args.verbose
    fast = args.fast

    mode = Mode.CHECK if args.check else Mode.PRUNE

    counter = Counter(Lock())
    num_cores = multiprocessing.cpu_count()

    queue = Queue()
    for target_dir in args.target_dirs:
        if os.path.isdir(target_dir):
            queue.put(target_dir)

    procs = []
    for i in range(num_cores):
        procs.append(
            Process(target=process, args=(queue, counter, mode, args.dry_run))
        )

    start_time = perf_counter()

    for p in procs:
        p.start()

    for p in procs:
        p.join()

    if verbose or counter.get_deleted_messages() > 0:
        elapsed_time = perf_counter() - start_time
        elapsed_min = elapsed_time / 60
        elapsed_sec = elapsed_time % 60
        print()
        print(
            "Processed %d mailboxes with %d mails."
            % (counter.get_mboxes(), counter.get_messages())
        )
        print(
            "Deleted %d messages (%dK)."
            % (
                counter.get_deleted_messages(),
                int(counter.get_deleted_bytes() / KBFACTOR),
            )
        )
        print("Finished after %dm %ds." % (elapsed_min, elapsed_sec))
