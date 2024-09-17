#!/usr/bin/env python

import os 
import sys
from optparse import OptionParser


def united_copy(srcPath, destPath, suffix='.csv', mode='once'):
    os.system('mkdir -p ' + destPath)

    if mode == 'recursive':
        for x in os.listdir(srcPath):
            #print(x)
            srcFile = srcPath + '/' + x + '/part-00000*' # always begin with part-00000
            #srcFile = srcPath + '/' + x + '/part-*'
            destFile = destPath + '/' + x + suffix
            print(srcFile, destFile)
            os.system('cp ' + srcFile + ' ' + destFile)
    else:
        #srcFile = srcPath + '/part-*'
        srcFile = srcPath + '/part-00000*'
        # remove the possible '/' at tail
        x = srcPath.rstrip('/').split('/')[-1]
        #print(x)
        destFile = destPath + '/' + x + suffix
        print(srcFile, destFile)
        os.system('cp ' + srcFile + ' ' + destFile)


def main():
    parser = OptionParser('usage: %prog')
    parser.add_option('-s', '--srcpath', metavar='srcpath', help='srcpath for input')
    parser.add_option('-d', '--destpath', metavar='destpath', help='destpath for output')
    parser.add_option('-x', '--suffix', metavar='suffix', default='.csv', help='.csv or _header.csv')
    parser.add_option('-m', '--mode', metavar='mode', default='once', help='recursive or once')
    # process current directory once, or recursively visit sub-directories
    (options, args) = parser.parse_args()
    #print(options)
    united_copy(options.srcpath, options.destpath, options.suffix, options.mode)

if __name__ == '__main__':
    main()


