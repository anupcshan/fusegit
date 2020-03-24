FUSEGit
=======

FUSE filesystem representing a Git repository


Components
==========
- fusegit - Mount a Git filesystem at a directory
- fgit - CLI tool to interact with a mounted fusegit directory


How to build it
===============
- Install Go (1.12 or above) by following instructions at https://golang.org/dl/
- Clone the repo
```
  $ git clone https://github.com/anupcshan/fusegit
  $ cd fusegit
```
- Build binaries
```
  $ go install ./cmd/fusegit
  $ go install ./cmd/fgit
```


Status
======
Super duper experimental and incomplete. Usage of this tool may result in harm to your pets.

Each fusegit instance runs an HTTP server over a Unix socket. Path to the socket is encoded in ".fusegitctl" at the root of the repo mount. fgit autodetects this socket when executed inside the mounted directory.


Aspirations
===========
- Support extremely lightweight clone/mount for large repos ("clone" time in less than 10s in all cases)
- Support progressive loading of objects based on filesystem usage
- Support write operations and auxiliary commands (like diff, status etc) on large repos


How to run it
=============
- Mount a repo
```
  $ fusegit <repo url> <mount directory> # Note that this command hangs till its unmounted
  $ fusegit https://kernel.googlesource.com/pub/scm/git/git /mnt
  21:47:47.203834 main.go:76: Initiated clone
  21:48:28.109821 main.go:111: Completed clone
  21:48:28.109920 main.go:124: Master ref d9f6f3b6195a0ca35642561e530798ad1469bd41 refs/remotes/origin/master
```
  At this point, `/mnt` will appear to have a readonly checkout of the repo at `master` commit. You
can inspect it via a file manager or view files (as you would normally do). All write attempts will
fail.

  A bare .git directory corresponding to this repo is present in `~/.cache/fusegit/<opaque_checksum>`.
This is preserved between mounts and speeds up subsequent mount operations. There is no "clone" step
if the .git directory exists - it is immediately mounted - see `fgit` operations below to see how to
perform "fetch".

  Crash recovery handling while interacting with this .git directory is poor (non-existent). If you
run into errors mounting, the best course of action is to `rm -r ~/.cache/fusegit` and start over.

- Interact with mounted repo
```
  $ fgit status
  d9f6f3b6195a0ca35642561e530798ad1469bd41

  $ fgit log   
  d9f6f3b6195a0ca35642561e530798ad1469bd41
  28014c1084c0180c71af9ffaaca82a57e48b76b5
  57b530125e022de79f5f0b208bc0a5ee67c18b77
  c22f63c40f0a576f3938dfd26c976ec052aa7fe2
  5731ca365789a04faecc281be24ea2eb0e438215
  ...

  $ fgit fetch
  Fetch complete in 386.927115ms

  $ fgit checkout 28014c1084c0180c71af9ffaaca82a57e48b76b5
  Checkout to 28014c1084c0180c71af9ffaaca82a57e48b76b5 complete in 2.213606ms
```

- Unmount a repo
```
  $ fusermount -u /mnt
```
