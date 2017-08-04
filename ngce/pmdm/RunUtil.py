'''
Created on Feb 12, 2016

@author: eric5946
'''

import os
import time
import arcpy
import multiprocessing
import subprocess
import platform
import sys
import tempfile

PATH_PYTHON27_32= r"C:\Program Files (x86)\PYTHON27\ArcGIS10.5"
PATH_PYTHON27_64= r"C:\Program Files (x86)\PYTHON27\ArcGISx6410.5"
WMX_TOOLS = r"\\aiotxftw6na01data\SMB03\elevation\WorkflowManager\Tools"

def runTool(path, toolArgs, bit32=False):
    path = r'"{}"'.format(os.path.join(WMX_TOOLS, path))

    for index in range(0,len(toolArgs)):
        if toolArgs[index].endswith('\\'):
            toolArgs[index] = toolArgs[index][0:-1]
        toolArgs[index] = r'"{}"'.format(toolArgs[index])
    
    if not bit32:
        arcpy.AddMessage("Architecture='{} {}' Python='{}'".format(arcpy.GetInstallInfo()['ProductName'],platform.architecture()[0],sys.executable))

    path_python27 = PATH_PYTHON27_64
    if bit32:
        path_python27 = PATH_PYTHON27_32

    env = os.environ.copy()
    env['PYTHONPATH']= r'{}\Lib\site-packages;{}'.format(path_python27, WMX_TOOLS)
    env['PATH']= path_python27
    exe = r'"{}\pythonw.exe"'.format(path_python27)        
    
    logfile = tempfile.NamedTemporaryFile(suffix=".log",dir=r"{}\Logs".format(WMX_TOOLS), delete=False)
    args = [exe,path]
    for arg in toolArgs:
        args.append(arg)
    args = " ".join(args)
    if not bit32:
        arcpy.AddMessage(args)
    # Error writing data to STDOUT, TODO: Switch to file logging
    #proc= subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, shell=False)
    proc= subprocess.Popen(args, env=env, shell=False, stdout=logfile, stderr=logfile )
    while proc.poll() is None:
        time.sleep(1)

    out,err = proc.communicate(None)
    retCode = proc.returncode

    
    if out is not None and len(out) > 0:
        arcpy.AddMessage(out)
    if err is not None and len(err) > 0:
        if retCode != 0:        
            arcpy.AddError(err)
        else:
            arcpy.AddWarning(err)
    
    if retCode != 0:  
        arcpy.AddError("'{}' failed with return code {}".format(path, retCode))
    elif not bit32:
        arcpy.AddMessage("{} Succeeded!".format(path))

    logfilepath = logfile.name
    logfile.close()

    ret = ''
    with open(logfilepath,'r') as lf:
        for line in lf:
            if len(str(line)) > 2:
                arcpy.AddMessage("{}".format(str(line).rstrip('\n').rstrip('\r').rstrip('\n')))
                ret = line

    if out is not None:
        return out
    else:
        return ret
