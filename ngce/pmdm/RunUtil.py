'''
Created on Feb 12, 2016

@author: eric5946
'''

import arcpy
import os
import platform
import subprocess
import sys
import tempfile
import time


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
    # Error writing data to STDOUT, Switched to file logging
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
    
    logfilepath = logfile.name
    logfile.close()

    if retCode != 0:  
        arcpy.AddError("'{}' failed with return code {}. Details in log file {} ".format(path, retCode, logfilepath))
    elif not bit32:
        arcpy.AddMessage("{} Succeeded! Details in log file {}".format(path, logfilepath))

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



def runToolx64_async(path, toolArgs, logpre="", logpath=None):
    if logpath is None:
        logpath = WMX_TOOLS
        
    path = r'"{}"'.format(os.path.join(WMX_TOOLS, path))

    for index, item in enumerate(toolArgs):
        if str(item).endswith('\\'):
            toolArgs[index] = item[0:-1]
        
        toolArgs[index] = r'"{}"'.format(toolArgs[index])
    
    path_python27 = PATH_PYTHON27_64
    
    env = os.environ.copy()
    env['PYTHONPATH'] = r'{}\Lib\site-packages;{}'.format(path_python27, WMX_TOOLS)
    env['PATH'] = path_python27
    exe = r'"{}\pythonw.exe"'.format(path_python27)        
        
    logpath = os.path.join(logpath, 'logs')
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    logfile = tempfile.NamedTemporaryFile(prefix=logpre, suffix=".log", dir=logpath, delete=False)
    
    args = [exe, path]
    for arg in toolArgs:
        args.append(arg)
    args = " ".join(args)
    
    # arcpy.AddMessage(args)
    
    # Error writing data to STDOUT, Switched to file logging
    # proc= subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, shell=False)
    proc = subprocess.Popen(args, env=env, shell=False, stdout=logfile, stderr=logfile)
    return proc, logfile
    

def endRun_async(path, proc, logfile):
    out, err = proc.communicate(None)
    retCode = proc.returncode

    
    if out is not None and len(out) > 0:
        arcpy.AddMessage(out)
    if err is not None and len(err) > 0:
        if retCode != 0:        
            arcpy.AddError(err)
        else:
            arcpy.AddWarning(err)

    logfilepath = logfile.name
    logfile.close()

#     ret = ''
#     with open(logfilepath, 'r') as lf:
#         for line in lf:
#             if len(str(line)) > 2:
#                 arcpy.AddMessage("{}".format(str(line).rstrip('\n').rstrip('\r').rstrip('\n')))
#                 ret = line

    if retCode != 0:  
        arcpy.AddError("ERROR: '{}' failed with return code {}. Details in log file {} ".format(path, retCode, logfilepath))
    else: 
        arcpy.AddMessage("SUCCESS: '{}' succeeded. Details in log file {}".format(path, logfilepath))
        
    return retCode
