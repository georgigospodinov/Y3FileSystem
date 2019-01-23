#!/usr/bin/env bash
_user="$USER"

# This function is used to facilitate reading the command line output.
run_cmd() {
    echo "$1"  # Show the command
    $1  # Execute it
    echo "____________________________________________"  # Separate output from other commands.
}

# Clear previous mounts
run_cmd "fusermount -u /cs/scratch/$_user/mnt"
run_cmd "make clean"
run_cmd "rm -rf /cs/scratch/$_user/mnt"

# Mount FS
run_cmd "mkdir /cs/scratch/$_user/mnt"
run_cmd "make"
run_cmd "./myfs -s /cs/scratch/$_user/mnt"

# Go into mount root
run_cmd "cd /cs/scratch/$_user/mnt"

# Creating and listing files & directories
run_cmd "touch a"
run_cmd "stat a"
run_cmd "touch b"
run_cmd "stat b"
run_cmd "mkdir dir"
run_cmd "stat dir"
run_cmd "ls -l"

# Change permissions
echo "echo 'HELLO'" > hi.sh
run_cmd "./hi.sh"
run_cmd "chmod 751 hi.sh"
run_cmd "./hi.sh"
echo 'permission' > p
run_cmd "chmod 000 p"
run_cmd "cat p"
run_cmd "chmod 700 p"
run_cmd "cat p"

# Hard links
echo 'test' > a
run_cmd "link a c"
run_cmd "stat a"
run_cmd "cat c"
echo 'zzzzzz' >> c
run_cmd "link c dir/file"
echo 'xxxxx' >> dir/file
run_cmd "cat a"

# Deletion
run_cmd "rm a"
run_cmd "ls"
run_cmd "stat c"
run_cmd "rm c"
run_cmd "stat dir/file"

# Deeper tree
run_cmd "mkdir dir/folder"
run_cmd "mkdir dir/folder/further"
run_cmd "cd dir/folder/further"
echo 'Hello1' > hello1.txt
echo 'Hello2' > hello2.txt
run_cmd "cd .."
run_cmd "cat further/hello*"

# Moving and renaming
run_cmd "mkdir closer"
run_cmd "mv further/hello2.txt closer/h2"
run_cmd "cat closer/h2"
run_cmd "cd ../.."
echo 'text' > file.txt
run_cmd "mv file.txt f"
run_cmd "cat f"

# Soft links
echo "text" > b
run_cmd "ln -s b c"
run_cmd "stat c"
echo "line2" >> c
run_cmd "ln -s c d"
run_cmd "cat d"

# Many touches
for i in {1..100}
do
    touch /cs/scratch/gg50/mnt/a$i
done
run_cmd "stat ."