#!/bin/bash

# ##### User Editable section #####
# 
# Directory locations.  The script won't make these, it expects them to already be there.
SCRIPT_DIR="/home/nfdump"
IMPORT_DIR="/home/nfdump/import"
HDFS_DIR="/user/nfdump"
# File locations used during operations.  Don't put your own files in the locations chosen here, or they WILL be overwritten.
LOCKFILE="./import_lock"
FIFO_FILE="./import_fifo"
# Nfdump output format:
OUT_FMT="fmt:%ra,%ts,%te,%pr,%sa,%da,%sp,%dp,%pkt,%byt,%flg,%tos"
#
# ##### Don't edit below this line. #####

# Don't retain wildcards that don't expand to anything, for safety's sake.
shopt -s nullglob

# Acquire the lockfile or die.
cd "$SCRIPT_DIR"
lockfile-create -l -r 0 "$LOCKFILE" || exit 1

# If things go wrong now we have the lockfile, we need to take care of fixing that.  Sure, we can't trap 9, but if someone does send us that signal, they'd better clean it up for us.
trap "[ -f $SCRIPT_DIR/$LOCKFILE ] && /bin/rm -f $SCRIPT_DIR/$LOCKFILE" 0 1 2 3 13 15 

if [ ! -p "$FIFO_FILE" ]; then
    # Check where the FIFO will be.  Is it a pipe?  No?  Well, it's either nothing, or a non-pipe.
    if [ -a "$FIFO_FILE" ]; then
        # Non-pipes should be deleted.
        rm "$FIFO_FILE"
    fi
    # Put the FIFO where it should be.
    mkfifo "$FIFO_FILE"
fi

if [ "$(ls -A $IMPORT_DIR)" ]; then
    # There are files to be imported.
    cd "$IMPORT_DIR"
    for f in *.nfcapd
    do
        hdfs fs -put "$FIFO_FILE" "$HDFS_DIR/$f.csv"& # Open the named pipe for reading.
        nfdump -r "$f" -o "$OUT_FMT" -q -N > "$FIFO_FILE" # Then write to it, blocking.
        rm "$f" #Then we're safe to delete.
    done
fi

# Now go back to where the lockfile was and 'unlock' the process by just deleting the file.
cd "$SCRIPT_DIR"
rm -f "$LOCKFILE"
