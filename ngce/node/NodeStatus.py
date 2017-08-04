'''
Created on Dec 11, 2015

@author: eric5946
'''
from datetime import datetime
import os


def timeDif(times=None):
    curTime = datetime.now()
    curTimes = os.times()
    
    difTime = 0
    difUser = 0
    difSystem = 0
    difTotal = 0
    
    difUserNorm = 0
    difSystemNorm = 0
    difTotalNorm = 0
    
    if times is not None:
        startTime = times[0]        
        startTimes = times[1]
        if startTime is not None and startTimes is not None:
            difTime = ((curTime - startTime).microseconds / 60000.0)
            difUser = curTimes[0] - startTimes[0]
            difSystem = curTimes[1] - startTimes[1]
            difTotal = difUser + difSystem
            
            if difTime > 0:
                difUserNorm = difUser / difTime
                difSystemNorm = difSystem / difTime
                difTotalNorm = difTotal / difTime
    
    
    return [curTime, curTimes, difTime, difUser, difSystem, difTotal, difUserNorm, difSystemNorm, difTotalNorm]
