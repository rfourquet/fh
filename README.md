# fh - cached sizes and hashes

## Sizes

This is similar to the `du` tool, but caches the result for
directories, so that repeated incantations can be fast. But unlike
`du`, `fh` reports "apparent size" by default instead of disk usage
(for directories, this is simply the sum of the apparent sizes of the
recursively contained files).

Other similar or related tools:
[`sizes`](https://github.com/jwiegley/sizes),
[`ncdu`](https://dev.yorhel.nl/ncdu), [`duc`](http://duc.zevv.nl/).

## Hashes

Currently only SHA1 hashes are supported. For directories, the hashes
of contained files and directories are combined together with
filenames (and optionally file modes) to produce a unique signature.
The main purpose is to easily detect duplicate directories (with the
same recursive content). While a "find duplicate" tool can be built on
top, `fh` still is useful when the two directories to compare are not
located on the same machine.

## Usage

This is the help message:

```
Usage: fh [-?|--long-help] [-x|--sha1] [-#|--hid] [-s|--size] [-d|--disk-usage]
          [-n|--count] [-c|--total] [-R|--depth INT] [-l|--cache-level INT]
          [-m|--mtime] [-L|--dereference] [-A|--deref-annex] [-u|--unique]
          [-X|--trust-annex] [-M|--use-modes] [-t|--si] [-z|--minsize INT]
          [-k|--mincount INT] [-S|--sort] [-D|--sort-du] [-N|--sort-count]
          [-G|--ignore-git] [--init-db PATH] [-I|--files-from FILE] [PATHS...]
  compute and cache the sha1 hash and size of files and directories

Available options:
  -h,--help                Show this help text
  -?,--long-help           show help for --cache-level and --init-db options
  -x,--sha1                print sha1 hash (in hexadecimal) (DEFAULT)
  -#,--hid                 print unique (system-wide) integer ID corresponding
                           to sha1 hash (use twice to reset the counter)
  -s,--size                print (apparent) size (DEFAULT)
  -d,--disk-usage          print actual size (disk usage) (EXPERIMENTAL)
  -n,--count               print number of (recursively) contained files
  -c,--total               produce a grand total
  -R,--depth INT           report entries recursively up to depth INT
  -l,--cache-level INT     policy for cache use, in 0..3 (default: 1 or 2)
  -m,--mtime               use mtime instead of ctime to interpret cache level
  -L,--dereference         dereference all symbolic links
  -A,--deref-annex         dereference all git-annex symbolic links
  -u,--unique              discard files which have already been accounted for
  -X,--trust-annex         trust the SHA1 hash encoded in a git-annex file name
  -M,--use-modes           use file modes to compute the hash of directories
  -t,--si                  use powers of 1000 instead of 1024 for sizes
  -z,--minsize INT         smallest size to show, in MiB
  -k,--mincount INT        smallest count to show
  -S,--sort                sort output, according to size
  -D,--sort-du             sort output, according to disk usage
  -N,--sort-count          sort output, according to count
  -G,--ignore-git          ignore ".git" filenames passed on the command line
  --init-db PATH           create a DB directory at PATH and exit
  -I,--files-from FILE     a file containing paths to work on ("-" for stdin)
  PATHS...                 files or directories (default: ".")
```

and the additional help (`--long-help`) for two options:
```
--cache-level L: the cache (hash and size) is used if:
   * L ≥ 1 and the timestamp of a file is compatible,
   * L ≥ 2 and the timestamp of a directory is compatible, or
   * L = 3.
 The timestamp is said "compatible" if ctime (or mtime with the -m option)
 is older than the time of caching.
 When the size of a file has changed since cached, the hash is unconditionally
 re-computed (even when L = 3).
 The default value of L is
   * 1 when hashes are requested (should be reliable in most cases)
       or when the unique option is active (it's otherwise easy to get
       confusing results with L = 2), and
   * 2 when only the size is requested (this avoids to recursively traverse
       directories, which would then be no better than du).

 --init-db P: three locations are checked for the database directory,
 in this order:
   * at the root of a device (e.g. /mnt/disk)
   * at /var/cache/
   * at the XDG cache directory (usually ~/.cache/), created by default
 The first of these allowing reading and writing is selected.
 The first two are only created on demand, with --init-db P,
 when P = /var/cache or e.g. P = /mnt/disk respectively.
 ```

## Examples

By default, hashes and sizes are reported:

```
$ fh
ace84a8949a382a820d2be148ee13e338e09520c   34.5k  .

$ fh -c *
9468ea7acbd21c9d0817bfa0a688678bf3e97f6b    9.0k  DB.hs
3a8f78621c62e3dac3d6bfa9c0d3b60f90638aaa   23.9k  Main.hs
a7020599cc3e029cd5e7e2c1e08eebf59adc3844    1.2k  Mnt.hs
22e4c93cb247913f18a014a148c835422df29f9c  542     Stat.hs
ace84a8949a382a820d2be148ee13e338e09520c   34.5k  *total*
```

Note that the above total reports the same hash and size as the first
command for the current directory. This behavior is also true for the
`--count` option, but not for "disk usage" (`-d`):

```
$ fh -sdn .
 48.0k   34.5k           (4)  .

$ fh -sdnc *
 12.0k    9.0k                DB.hs
 24.0k   23.9k                Main.hs
  4.0k    1.2k                Mnt.hs
  4.0k  542                   Stat.hs
 44.0k   34.5k           (4)  *total*
```

Using a cryptographic hash is a straightforward way to compare for
equality two files (or directories, with `fh`), but to be honest, when
I do that manually, I will generally compare only few digits only of
the hash digest, which is not safe when I intend to delete one of the
copies. Here comes the `--hid` (`-#` for short) option, which reports
a system-wide unique small integer in correspondence with a given
hash: this is easier to visually compare:

```
$ fh -# .
   #1  .

$ fh -#c *
   #2  DB.hs
   #3  Main.hs
   #4  Mnt.hs
   #5  Stat.hs
   #1  *total*

$ cp DB.hs DB2.hs

$ fh -#x DB*
   #2  9468ea7acbd21c9d0817bfa0a688678bf3e97f6b  DB2.hs
   #2  9468ea7acbd21c9d0817bfa0a688678bf3e97f6b  DB.hs
```

## Specifying the paths

The simplest way is to provide the paths on the command line. Programs
like `xargs` and e.g. `find` will give quite some flexibility, but due
to a current limitation of the code parsing the arguments, specifying
too many paths arguments on the command line has some inefficiency.

Moreover, there are some built-in limits as to how many arguments a
command can have. Also, above a certain threshold, `xargs` will by
default split the arguments and invoke the command repeatedly, which
can have confusing results: for example when a sorting option is used, the
result will appear to be sorted by chunks instead of globally.

A solution is to use the `--file-from` option, in particular by specifying
`-` as the file, to read the list of paths from `stdin`.
Note that the `--ignore-git` option applies also to those files.

For example:

```
$ cd /nix/store/aaa9* && find -depth -type d | fh -dnu -k1 -I-
332.0k           (9)  ./share/doc/storable-record-0.0.4/html/src
164.0k          (16)  ./share/doc/storable-record-0.0.4/html
  8.0k           (1)  ./share/doc/storable-record-0.0.4
```

The `-depth` option makes `find` print a directory after its content
has been printed; then the `-u` and `-k1` options of `fh` allow to
filter out non-leaf directories for which all content has already been
accounted for.

The `--depth` option of `fh` also covers some use cases (with the
`--ignore-git` option, ".git" directories _and_ their content are
omitted from the list of generated paths).

## For git-annex users

"Annex'ed" directories are full of `git-annex` symbolic links, so the
reported sizes are not very meaningful. Like with `du`, it's possible
to use the `-L` option to follow symlinks, but there is a more
specific option to follow only `git-annex` symlinks: `-A`. Also, as
`git-annex` itself computes hashes, it can be wasteful for `fh` to
recompute these when using SHA1, so there is an option to trust SHA1
hashes computed by git-annex: `-X` (this must be used in combination
with `-A` to be useful, unless `fh` is run directly against the
`git-annex` store, at ".git/annex/objects/").

## Compatibility and dependency

`fh` currently relies on the `findmnt` program (which is available at
least on GNU/Linux), and depends on POSIX features.
