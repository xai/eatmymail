#!/usr/bin/env python3

import sys
import os
import glob
import mailbox
import hashlib

def print_usage(path):
    print("Usage: %s [MAILDIR]")

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
            print("  deleting \"%s\" (%s bytes, %s)" \
                    % (message['Subject'], message['Content-Length'], hashsum))
            mbox.remove(key)
    finally:
        mbox.flush()
        mbox.close()

def prune(mbox):
    messages = {}
    to_remove = {}

    for key, message in mbox.iteritems():
        msgid = message['Message-Id']
        if msgid is None:
            continue

        if msgid in messages:
            messages[msgid].append(key)
        else:
            messages[msgid] = [key]

    # Check duplicate message ids
    # this is reasonably fast
    for msgid in messages:
        if len(messages[msgid]) > 1:
            dupes = {}
            for key in messages[msgid]:
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