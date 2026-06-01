#!/bin/bash
# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# This software (KWDB) is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
from os import path, getenv  # NOQA
from sys import path as syspath  # NOQA
syspath.append(getenv('QA_DIR'))  # NOQA
from sys import platform as sysplatform
import os as _os

from util.kwbase_exec_basic_v2 import NodeExecuter
import util.utils as utils
from util.master_compare_v2 import diff
from argparse import ArgumentParser
from typing import Tuple, List, Dict
from re import search as research, match as rematch
from glob import glob
from datetime import datetime
from threading import Lock
import subprocess as _subprocess


def shrink_sign(sign: str, token: str) -> str:
  return sign.lstrip(token).strip(" ").strip('\n')


FP = '-'*10


class SummaryFile(object):
  __summary_lck = Lock()

  def __init__(self, path: str) -> None:
    self.__summary_file = path

  def Write(self, content: str) -> None:
    try:
      SummaryFile.__summary_lck.acquire(blocking=True)
      utils.FileWrite(self.__summary_file, content=content + "\n", mode='a')
    finally:
      SummaryFile.__summary_lck.release()


class Executer(object):

  def __succ(self, s: str) -> None:
    cs = utils.ConsoleColored.Green(f'{datetime.now()}[ {FP} {s} {FP} ]')
    self.__summary.Write(cs)
    utils.direct_print(cs)

  def __fail(self, s: str) -> None:
    cs = utils.ConsoleColored.Red(f'{datetime.now()}[ {FP} {s} {FP} ]')
    self.__summary.Write(cs)
    utils.direct_print(cs)

  def __topology_match(self, node_name: str) -> bool:
    match = rematch(r'(\d+)([cn]r?)', self.__topology)
    if not match:
      return False
    groups = match.groups()
    if len(groups) != 2:
      return False
    node_match = rematch(r'([cn]r?)(\d+)', node_name)
    if not node_match:
      return False
    node_match_groups = node_match.groups()
    return groups[1] == node_match_groups[0] \
      and int(node_match_groups[1]) <= int(groups[0])

  def __init__(self, summary_file: str, topology: str, filter: str) -> None:
    self.__summary = SummaryFile(summary_file)
    self.__topology = topology
    self.__filter = filter
    self.__cur_exec_dir = ""

  def ExecuteCase(self, stmts: str, exec_nodes: Dict[str, NodeExecuter]) -> str:
    rc = ''
    testmode = [self.__topology]
    testnode = ['n1' if self.__topology.endswith(
        'n') else 'cr1' if self.__topology.endswith('cr') else 'c1']
    stmt = ''
    block_comment = False
    delimiter_tag = False
    for line in stmts.split('\n'):
      if line.lstrip().startswith('/*'):
        block_comment = True
      if line.rstrip().endswith('*/'):
        block_comment = False
        continue
      if block_comment:
        continue
      if line.lstrip().startswith('--'):
        if bool(research(r'^--[-\s]*test_?mode.*', line)):
          testmode = line.partition('mode')[2].strip().split(" ")
        if bool(research(r'^--[-\s]*test_?node.*', line)):
          testnode = line.partition('node')[2].strip().split(" ")
        continue
      if line.lstrip().startswith('delimiter \\\\'):
        stmt += line + '\n'
        delimiter_tag = True
        continue
      if line.lstrip().startswith('delimiter ;'):
        stmt += line + '\n'
        delimiter_tag = False
        if self.__topology in testmode:
          for node in testnode:
            if not self.__topology_match(node):
              utils.direct_print(f'node {node} not in {self.__topology}')
              continue
            enode = exec_nodes.get(node)
            if not enode:
              enode = NodeExecuter(path.join(getenv("DEPLOY_ROOT"), node))
              exec_nodes.setdefault(node, enode)
            rc += enode.Execute(stmt)
          stmt = ''   
        continue
      if delimiter_tag:
        stmt += line + '\n'
        continue
      stmt += line + '\n'
      if bool(research(r'.*;(\s*--.*)?$', line.rstrip())):
        if self.__topology in testmode:
          for node in testnode:
            if not self.__topology_match(node):
              utils.direct_print(f'node {node} not in {self.__topology}')
              continue
            enode = exec_nodes.get(node)
            if not enode:
              enode = NodeExecuter(path.join(getenv("DEPLOY_ROOT"), node))
              exec_nodes.setdefault(node, enode)
            rc += enode.Execute(stmt)
          stmt = ''
    return rc

  def ExecuteFile(self, sql_file: str, sql_dir: str) -> List[Tuple[bool, str]]:
    _exec_nodes: Dict[str, NodeExecuter] = dict({})
    rs = list()
    for scope in utils.SpiltFile(sql_file, re_pattern=r'^--[-\s]*test_?case.*'):
      if all([s.startswith('--') or not s.strip() for s in scope[1].split('\n')]):
        continue
      case_text = ''
      case_name = ''
      if scope[0]:
        case_text = '@' + scope[0].partition("case")[2].strip()
        case_name = case_text.strip().partition(' ')[0]
      log_case = f'{sql_file}({self.__topology}){case_text}'
      self.__succ(f'Execute {log_case}')
      output = self.ExecuteCase(scope[1], exec_nodes=_exec_nodes)
      output_path = path.join(
          getenv('QA_DIR'),
          'TEST_integration', sql_dir,
          f'{path.basename(sql_file)}_{self.__topology}{case_name}.out')
      expect_output = getenv('EXPECT_OUTPUT')
      if expect_output is None or expect_output=='':
        master_file = path.join(
            path.dirname(sql_file),
            'master',
            f'{path.basename(sql_file)}_{self.__topology}{case_name}.master')
      else:
        master_file = path.join(
            path.dirname(sql_file),
            'master',
            f'{path.basename(sql_file)}_{self.__topology}{case_name}_{expect_output}.master')
        if not path.exists(master_file):           
          master_file = path.join(
              path.dirname(sql_file),
              'master',
              f'{path.basename(sql_file)}_{self.__topology}{case_name}.master')
      if getenv("OVERWRITE_MASTER") == "yes":
        output_path = master_file
      utils.FileWrite(output_path, output)

      diffout = diff(master_file=master_file, out_file=output_path)
      if len(diffout) > 0:
        self.__fail(f'Finished {log_case} FAILED')
        utils.direct_print(
            f'Difference between {master_file} and {output_path}')
        utils.direct_print(diffout)
        if not getenv("OVERWRITE_MASTER") == "yes":
          utils.FileWrite(file_path=master_file + '.diff', content=diffout)
        rs.append((False, log_case))
      else:
        self.__succ(f'Finished {log_case} PASSED')
        rs.append((True, log_case))
    _exec_nodes.clear()
    return rs

  # WIndows only, True is any failure detected
  def getOutputFilename(self, sql_file):
    return getenv("DEPLOY_ROOT") + _os.sep + path.basename(sql_file) + '_' + self.__topology +'.out'
  

  def getExpFilePrefix(self, sqlFn):
    return  getenv("QA_DIR") + _os.sep + "Integration" + _os.sep + self.__cur_exec_dir + _os.sep + "master" + _os.sep +  path.basename(sqlFn) + '_' + self.__topology 
  
  def getExpFileFn(self, sqlFn):
    return self.getExpFilePrefix(sqlFn) +'.master'
    

  # Windows only, fn is the output file
  # return True if error detected
  def checkServerFailureInRun(self, fn):
    cmd = f"grep \"the server is running\" {fn}"
    result = _subprocess.run(cmd,  shell=True, capture_output=True, text=True,  encoding='utf-8')
    if result.returncode == 0 :
      return True

  # Windows only
  def checkWinExecFailure(self, tup):
    rc = tup[0]
    file = tup[1]
    if rc == False:
      print ("Error running test file", file, ", exiting.")
      return True
    outFile = self.getOutputFilename(file)
    if self.checkServerFailureInRun(outFile):
      return True
    return False

  # Windows only.  Retrun true if exp file is in place, or sccessfully flatten'ed
  def flattenExpIfNeeded(self, sqlFn):
    exp = getMingwPath(self.getExpFilePrefix(sqlFn))
    cmd = f"ls  {exp}* | wc -l"
    r = wrappredExec(cmd)
    if r.stdout.rstrip() == "1":
      return True   # only 1 file consider flatten'ed
    else:
      expFn = getMingwPath(self.getExpFileFn(sqlFn))
      cmd2 = f"cat {exp}@* > {expFn}"
      r = wrappredExec(cmd2)
      if r.returncode == 0:
        return True
      else:
        print("Not able to flatten expect file" + expFn)
        return False


  # for Widnows only, input is SQL file, use diff to compare exp and out
  # return True if output matches exp
  def ckeckExpectation(self, sqlFile):
    outFn = getMingwPath(self.getOutputFilename(sqlFile))
    expFn = getMingwPath(self.getExpFileFn(sqlFile))
    rc = self.flattenExpIfNeeded(sqlFile)
    if rc != True:
      return False

    if diffFile(outFn, expFn):
      print("Detected run output difference: " + outFn + " " + expFn)
      return True
    return True

  # On Widnows, we do file-wise execution with kwbase sql command
  # All the paths should be in windows format
  # I.e. we don't split cases.   TODO: the code below only runs each file and
  # dump output to a file.  There should be some logic in upper level to handle result
  # comparison, etc
  def ExecuteFileWin(self, sql_file: str, sql_dir: str) -> List[Tuple[bool, str]]:
    node = 'n1'
    f = open(path.join(path.join(getenv("DEPLOY_ROOT"), node), 'kwbase.listen-addr'))
    host = f.readline()
    outfile = self.getOutputFilename(sql_file) 
    kwbin = _os.getenv('KWBIN')
    # make it work in mingw env
    sql_file = path.abspath(sql_file)
    
    cmd0 = f"{kwbin} sql --insecure --host={host} --format=table --set \"errexit=false\" --echo-sql --set \"show_times=false\""
    cmd1 = cmd0 + " < " + sql_file + " > " + outfile + " 2>&1"
    print ("\n"+cmd1)
    result = wrappredExec(cmd1)
    rs.append((result.returncode == 0, sql_file))
    if self.checkWinExecFailure(rs[len(rs)-1]):
      print(result)
    return rs

  def ExecuteDir(self, sql_root: str, sql_dir: str) -> List[Tuple[bool, str]]:
    rs = list()
    self.__cur_exec_dir = sql_dir
    sql_files = glob(path.join(sql_root, self.__filter))
    sql_files.sort()
    for sql_file in sql_files:
      if sysplatform == 'win32':
        rs += self.ExecuteFileWin(sql_file, sql_dir=sql_dir)
        # the actucal error checking was done inside of ExecuteFileWin, just stop the loop here
        if rs[len(rs)-1][0] == False:
          break
        if not self.ckeckExpectation(sql_file):
          break
      else:
        rs += self.ExecuteFile(sql_file, sql_dir=sql_dir)
    self.__cur_exec_dir = ""
    return rs

def getMingwPath(fn):
  cmd = f"cygpath -m {fn}"
  result = wrappredExec(cmd)
  if result.returncode == 0 :
    return result.stdout.rstrip()
  else:
    return ""
  
def wrappredExec(cmd):
  return _subprocess.run(cmd,  shell=True, capture_output=True, text=True, encoding='utf-8')

def diffFile(f1, f2):
  cmd = f"diff {f1} {f2}"
  result = wrappredExec(cmd)
  if result.returncode != 0 :
    return True
  else:
    return False

if __name__ == "__main__":
  parser = ArgumentParser(path.basename(__file__))
  parser.add_argument("-t", dest="topology", type=str)
  parser.add_argument("-f", dest="filter", type=str)
  parser.add_argument("-d", dest="sql_dirs", type=str, nargs="+")
  parser.add_argument("-s", dest="summary_file", type=str)
  args = parser.parse_args()

  rs: List[Tuple[bool, str]] = list()

  ee = Executer(summary_file=args.summary_file, topology=args.topology, 
                filter=args.filter)
  for sql_dir in args.sql_dirs:
    rs += ee.ExecuteDir(path.join('Integration', sql_dir), sql_dir=sql_dir)

  total = len(rs)
  passed_cnt = len([r for r in rs if r[0]])
  SummaryFile(args.summary_file).Write(
      f'SummaryResult: {passed_cnt} {total - passed_cnt} {total}')
