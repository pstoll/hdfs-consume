#!env python
import subprocess
import logging


###################################################
def du_recursive (path, threshold, level=0, maxlevel = 3, cmd = "sudo -u hdfs hdfs dfs -du", interactive=True):
  """
  Returns list of directories with sizes, which are bigger, then _threshold_ (in bytes)
  path: the root path to start with
  threshold: min size of directory (bytes)
  level, maxlevel: current level and limit for recurse
  cmd: command for run hdfs du subprocess

  '/' -> [{'path':'/dir1/subdir1', 'size':'32546'},
          {'path':'/dir2/subdir2', 'size':'6865436'},
          {'path':'/dir3/subdir3', 'size':'565564'}]
  """
  dirs = []
  logging.debug('Doing %s %s...' % (cmd, path))
  if interactive:
    from sys import stdout
    stdout.write('\x1b[2KScanning %s...\r' % (path)) # "\x1b[2K" deletes current line
    stdout.flush()
  try:
    result = subprocess.check_output('%s %s' % (cmd, path), stderr=subprocess.STDOUT, shell=True)
  except subprocess.CalledProcessError as e:
    logging.error('Process %s returns code %s, Output: \n%s' % (e.cmd, e.returncode, e.output))
    return []

  for line in result.split('\n'):
    try:
      [size, filename] = line.split(' ',1)
      filename = filename.strip()
      logging.debug('file:%s, size:%s' % (filename, size))
    except ValueError:
      logging.warning('Cannot parse line:%s' % line)
      continue
    if int(size) > threshold:
      logging.debug('%s is more then %s' % (humansize(int(size)), humansize(threshold)))
      d = dict(size=int(size), path=filename)
      if level < maxlevel:
        logging.debug('We need to go deeper (level=%d)' % (level+1))
        dirs.extend(du_recursive(filename, threshold, level=level+1, interactive=interactive))
      else:
        dirs.append(d)
  return dirs

def humansize(nbytes):
  """
  Converts size in bytes to human-readable format
  68819826 -> 65.63M
  39756861649 -> 37.03G
  
  (C) http://stackoverflow.com/users/1204143/nneonneo
  """
  suffixes = ['B', 'K', 'M', 'G', 'T', 'P']
  if nbytes == 0: return '0'
  i = 0
  while nbytes >= 1024 and i < len(suffixes)-1:
      nbytes /= 1024.
      i += 1
  f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
  return '%s%s' % (f, suffixes[i])

################################################

if __name__ == '__main__':

  import argparse
  parser = argparse.ArgumentParser ( prog = 'python hdfs-consume.py',
        description = 'Print list of top hdfs consumers')

  parser.add_argument ( '--version', action='version', version='HDFS Consume v.0.1' )
  parser.add_argument ( 'path', help='Root path to scan', default="/" )
  parser.add_argument ( '--threshold', '-t', help='Minimum dir size to show (in bytes)', default=300*1024*1024*1024 ) # 300G
  parser.add_argument ( '--depth', '-d', help='Max directory level to scan', default=3 )
  parser.add_argument ( '--log', '-l', help='Logfile name', metavar='FILENAME', default='/tmp/hdfs-consume.log' )
  parser.add_argument ( '--verbosity', '-v', action='count', help='Logging level, use -vvvv to debug', default=1 ) # logging.ERROR == 40,
  parser.add_argument ( '--cmd', help='Command string for running "hdfs -du" as subprocess', default='sudo -u hdfs hdfs dfs -du' )
  parser.add_argument ( '--output', '-o', help="Output filename", required=True )

  config = parser.parse_args()

  logging.basicConfig(filename = config.log, level=50-10*config.verbosity)
  logging.debug(config)

  sorted_list = sorted(
                       du_recursive(config.path, config.threshold, maxlevel=config.depth, cmd=config.cmd, interactive=True),
                       key=lambda k: k['size'],
                       reverse=True)

  f = open (config.output, 'w')
  for item in sorted_list:
    f.write( '%-10s%s\n' % (humansize(item['size']), item['path']))
