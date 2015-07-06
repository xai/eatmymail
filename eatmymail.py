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

import sys
import mailbox
import hashlib


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


def remove(mbox, to_remove):
    mbox.lock()
    try:
        for key, hashsum in to_remove.items():
            message = mbox.get(key)
            print("  deleting \"%s\" (%s bytes, %s)" % (message['Subject'], message['Content-Length'], hashsum))
            mbox.remove(key)
    finally:
        mbox.flush()
        mbox.close()


def prune(mbox):
    messages = {}
    to_remove = {}

    for key, message in mbox.iteritems():
        message_id = message['Message-Id']
        if message_id is None:
            continue

        if message_id in messages:
            messages[message_id].append(key)
        else:
            messages[message_id] = [key]

    # Check duplicate message ids
    # this is reasonably fast
    for message_id in messages:
        if len(messages[message_id]) > 1:
            dupes = {}
            for key in messages[message_id]:
                message = mbox.get(key)
                hashsum = hash_content(message)
                if hashsum in dupes:
                    dupes[hashsum].append(key)
                else:
                    dupes[hashsum] = [key]

            # Check duplicate hashes
            # this should be reasonably precise
            for hashsum in dupes:
                if len(dupes[hashsum]) > 1:
                    for key in dupes[hashsum][1:]:
                        to_remove[key] = hashsum

    remove(mbox, to_remove)

    for subdir in mbox.list_folders():
        print('Subdir found: %s', subdir)
        prune(subdir)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print_usage(sys.argv[0])
        exit(1)

    for target in sys.argv[1:]:
        mbox = mailbox.Maildir(target)
        prune(mbox)
