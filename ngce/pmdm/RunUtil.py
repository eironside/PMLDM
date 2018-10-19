'''
Created on Feb 12, 2016

@author: eric5946
'''

import arcpy
import numpy
import os
import platform
import subprocess
import psutil
import sys
import tempfile
import time


# PATH_PYTHON27_32 = r"C:\Program Files (x86)\PYTHON27\ArcGIS10.5"
PATH_PYTHON27_32 = r"C:\Python27\ArcGIS10.5"

# PATH_PYTHON27_64 = r"C:\Program Files (x86)\PYTHON27\ArcGISx6410.5"
PATH_PYTHON27_64 = r"C:\Python27\ArcGISx6410.5"
# PATH_PYTHON27_64= r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3'

WMX_TOOLS = r"\\aiotxftw6na01data\SMB03\elevation\WorkflowManager\Tools"
# WMX_TOOLS = r"C:\Users\eric5946\workspaceEE\NGCE_PMDM\src-ngce"
# WMX_TOOLS = r'C:\Temp'

TOOLS_PATH = r"\\aiotxftw6na01data\SMB03\elevation\WorkflowManager\Tools\ngce\pmdm\a"
# TOOLS_PATH = r"C:\Users\eric5946\workspaceEE\NGCE_PMDM\src-ngce\ngce\pmdm\a"

PROD_TOOLS = r"C:\Program Files (x86)\ArcGIS\EsriProductionMapping\Desktop10.5\arcpyproduction"


def getLogFile(log_path, script_name):
    log_parts = os.path.split(log_path)
    if len(log_parts) >= 2 and (not str(log_parts[1]).upper() == "LOGS"):
        log_path = os.path.join(log_path, "Logs")
    if script_name is not None and len(str(script_name)) > 0:
        log_path = os.path.join(log_path, os.path.splitext(script_name)[0])
# #Override to local for testing, didn't make a difference
# log_path = r"C:\Temp\logs"
    #arcpy.AddMessage("Logs are written to folder: {}".format(str(log_path)))
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    logfile = tempfile.NamedTemporaryFile(
        prefix=script_name[:-3] + '_',
        suffix=".log",
        dir=log_path,
        delete=False)
    
    #try:
    #    arcpy.AddMessage("Logs are written to file: {}".format(str(logfile.name)))
    #except:
    #    pass
    
    return logfile

def runTool(path, toolArgs, bit32=False, log_path=WMX_TOOLS, profile=True):
    if log_path is None:
        log_path = WMX_TOOLS
        
    script_name = os.path.split(path)[1]
    path = r'"{}"'.format(os.path.join(WMX_TOOLS, path))
    
    if toolArgs is not None:
        for index in range(0, len(toolArgs)):
            if toolArgs[index] is not None and str(toolArgs[index]).endswith('\\'):
                toolArgs[index] = toolArgs[index][0:-1]
            toolArgs[index] = r'"{}"'.format(toolArgs[index])
    
    if not bit32:
        arcpy.AddMessage("Architecture='{} {}' Python='{}'".format(arcpy.GetInstallInfo()['ProductName'], platform.architecture()[0], sys.executable))

    path_python27 = PATH_PYTHON27_64
    if bit32:
        path_python27 = PATH_PYTHON27_32

    env = os.environ.copy()
    # env['PYTHONPATH'] = r'{}\Lib\site-packages;{};{}'.format(path_python27, WMX_TOOLS, PROD_TOOLS)
    env['PYTHONPATH'] = r'{}\Lib\site-packages;{}'.format(path_python27, WMX_TOOLS)
    env['PATH'] = path_python27
    exe = r'"{}\pythonw.exe"'.format(path_python27)
    # exe = path_python27

    logfile = getLogFile(log_path, script_name)
    
    args = [exe, path]
    if toolArgs is not None:
        for arg in toolArgs:
            args.append(arg)
    args = " ".join(args)
    if not bit32:
        arcpy.AddMessage(args)
    # Error writing data to STDOUT, Switched to file logging
    # proc= subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, shell=False)
    proc = subprocess.Popen(args, env=env, shell=False, stdout=logfile, stderr=logfile)

    # Check for Subprocess Completion & Optionally Capture System Profile Values
    sys_info = []
    sys_file = getLogFile(log_path, script_name + '_PROFILE___')
    while proc.poll() is None:
        if profile:
            cpu = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory().percent
            now = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
            sys_file.write('{} -- CPU %: {} | MEM %: {} \n'.format(now, cpu, mem))
            sys_info.append((cpu, mem))
            time.sleep(1)
        else:
        time.sleep(1)
    sys_file.close()

    # Write Profile Averages to Primary Log
    if profile:
        logfile.write('Median CPU %: {} '.format(numpy.median([_[0] for _ in sys_info])))
        logfile.write('Median MEM %: {} '.format(numpy.median([_[1] for _ in sys_info])))

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

    if retCode != 0:  
        arcpy.AddError("'{}' failed with return code {}. Details in log file {} ".format(path, retCode, logfilepath))
    elif not bit32:
        arcpy.AddMessage("{} Succeeded! Details in log file {}".format(path, logfilepath))

    ret = ''
    try:
        with open(logfilepath, 'r') as lf:
            for line in lf:
                if len(str(line)) > 2:
                    arcpy.AddMessage("{}".format(str(line).rstrip('\n').rstrip('\r').rstrip('\n')))
                    ret = line
    except:
        arcpy.AddMessage("Failed to open log file. please review it if it existst {}".format(logfilepath))

    if out is not None:
        return out
    else:
        return ret



def runToolx64_async(path, toolArgs, logpre="", logpath=None):

    if logpath is None:
        logpath = WMX_TOOLS
    script_name = os.path.split(path)[1]
    
    path = r'"{}"'.format(os.path.join(WMX_TOOLS, path))

    for index, item in enumerate(toolArgs):

        try:
            i = '"{}"'.format(toolArgs[index])

        except UnicodeEncodeError as e:
            try:
                i = '"{}"'.format(toolArgs[index].encode('utf-8'))
            except Exception as e:
                arcpy.AddMessage('Encoding Failed: {}'.format(e))
        
        if i.endswith('\\'):
            toolArgs[index] = i[0:-1]

        else:
            toolArgs[index] = i

        #toolArgs[index] = r'"{}"'.format(toolArgs[index])
    
    path_python27 = PATH_PYTHON27_64
    
    env = os.environ.copy()
    env['PYTHONPATH'] = r'{}\Lib\site-packages;{}'.format(path_python27, WMX_TOOLS)
    env['PATH'] = path_python27
    exe = r'"{}\pythonw.exe"'.format(path_python27)        
#         
#     log_parts = os.path.split(logpath)
#     if len(log_parts) >= 2 and (not str(log_parts[1]).upper() == "LOGS"):  
#         logpath = os.path.join(logpath, "Logs")
#     if script_name is not None and len(str(script_name)) > 0:
#         logpath = os.path.join(logpath, os.path.splitext(script_name)[0])
#         
#     # #Override to local for testing, didn't make a difference
#     # log_path = r"C:\Temp\logs"
#     if not os.path.exists(logpath):
#         os.makedirs(logpath)
#     
#     logfile = tempfile.NamedTemporaryFile(prefix=logpre, suffix=".log", dir=logpath, delete=False)
    logfile = getLogFile(logpath, script_name) 
    
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
    #else: 
        #arcpy.AddMessage("\tSUCCESS: '{}' succeeded. Details in log file {}".format(path, logfilepath))
        #arcpy.AddMessage("\tSUCCESS: '{}' succeeded.".format(path))
        
    return retCode
