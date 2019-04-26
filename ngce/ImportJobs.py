'''
Created on Jan 20, 2017

@author: eric5946
'''


import arcpy

import re

arcpy.CheckOutExtension('JTX')

filter_state = arcpy.GetParameterAsText(0)

# #Get a list of Hold types in Workflow database
# hold_types = conn.config.getHoldTypes()
#
# #Access a Workflow Job
# job = conn.getJob(99999)
#
# #Find the id of Budget Hold hold type and add a hold on the job
# for hold in hold_types:
#     if(hold.name=='Budget Hold'):
#         job.addHold(hold.id,comment="Insufficient funds for digitization.")

fc = r"Q:\Admin\ElevMetadataBackups\20170906\MetadataBoundaries.gdb\Nov2016_NAD83"
jtc = None#r'Q:\WorkflowManager\PM_LDM@AIOTXFTW3DB001_admin.jtc'
cmdr = None#r'C:\Users\Eric.Ironside\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\EDITvLDM_CMDR@AIOTXFTW3DB001@ARcgisadmin.sde'

jobTypeName=r'LDM AB - Publish Backlog Project'
dataWorkspaceID = '{29BD0483-43AB-495B-8259-A0C94071AFF5}'
assignedTo ='steven.nechero'
assignedType ='User'
parentVersionName='DBO.Edit'
#parentDirectory = r"\\aiotxftw6na01data\smb03\elevation\LiDAR" #Replaced by following line 22 Mar 2019 BJN
parentDirectory = r"\\aiotxftw6na01\smb03\elevation\LiDAR"
#archiveDirectory = r"\\aiotxftw6na01data\smb03\elevation\LiDAR" #Replaced by following line 22 Mar 2019 BJN
archiveDirectory = r"\\aiotxftw6na01\smb03\elevation\LiDAR"

fieldProjName='ProjName'
fieldHorzProj='HorzProj'
fieldHorzUnit='HorzUnit'
fieldVertProj='VertProj'
fieldVertUnit='VertUnit'
fieldNPS='NPS'
fieldPtDenMeter='PtDenMeter'
fieldRMSE='RMSE'
fieldCellResolu='CellResolu'
fieldBeginDate='BeginDate'
fieldEndDate='EndDate'
fieldMetaXMLLin='MetaXMLLin'
fieldInMscServ='InMscServ'
fieldUSGS='USGS'
fieldElevUnit='ElevUnit'
fieldUserGuide='UserGuide'
fieldBoundary='Boundary'
fieldHDDateRcvd='HDDateRcvd'
fieldCopy2Deliv='Copy2Deliv'
fieldWorkflowNotes='WorkflowNotes'
fieldSqMiles='SqMiles'
fieldDSMstatus='DSMstatus'
LasVersion = 'LasVersion'
LasClassified= 'LasClassified'
HydroFlat = 'HydroFlat'

contract_

conn = None
cursor = arcpy.SearchCursor(fc)
for row in cursor:
    print(row)

    loi = row.getValue("SHAPE")
    valueProjName = row.getValue(fieldProjName)
    valueHorzProj = row.getValue(fieldHorzProj)
    valueHorzUnit = row.getValue(fieldHorzUnit)
    valueVertProj = row.getValue(fieldVertProj)
    valueVertUnit = row.getValue(fieldVertUnit)
    valueNPS = row.getValue(fieldNPS)
    valuePtDenMeter = row.getValue(fieldPtDenMeter)
    valueRMSE = row.getValue(fieldRMSE)
    valueCellResolu = row.getValue(fieldCellResolu)
    valueBeginDate = row.getValue(fieldBeginDate)
    valueEndDate = row.getValue(fieldEndDate)
    valueMetaXMLLin = row.getValue(fieldMetaXMLLin)
    valueInMscServ = row.getValue(fieldInMscServ)
    valueUSGS = row.getValue(fieldUSGS)
    valueElevUnit = row.getValue(fieldElevUnit)
    valueUserGuide = row.getValue(fieldUserGuide)
    valueBoundary = row.getValue(fieldBoundary)
    valueHDDateRcvd = row.getValue(fieldHDDateRcvd)
    valueCopy2Deliv = row.getValue(fieldCopy2Deliv)
    valueWorkflowNotes = row.getValue(fieldWorkflowNotes)
    valueSqMiles = row.getValue(fieldSqMiles)
    valueDSMstatus = row.getValue(fieldDSMstatus)

    valueLasVersion = row.getValue(LasVersion)
    valueLasClassified = row.getValue(LasClassified)
    valueHydroFlat = row.getValue(HydroFlat)

    projectNameParts = str(valueProjName).split("_")
    year = projectNameParts[len(projectNameParts)-1]
    state = projectNameParts[0]
    aliasClean = "Project"
    if len(projectNameParts) >2:
        aliasClean = projectNameParts[1]

    alias =aliasClean
    aliasParts = re.findall('[A-Z][^A-Z]*', aliasClean)
    if len(aliasParts)>1:
        alias = " ".join(aliasParts)
    print "\t\t'{}' '{}' '{}'".format(state, alias, year)

    desc = {'LOI': loi,
            'assignedTo':assignedTo,
            'assignedType': assignedType,
            'dataWorkspaceID':dataWorkspaceID,
            'jobTypeName':jobTypeName,
            'parentVersionName': parentVersionName,
            'prefix':'',
            'versionName':''}

    if conn is None and jtc is not None:
        #Establish a connection to a Workflow database
        conn = arcpy.wmx.Connect(jtc)

    if conn is not None and state == filter_state:
        #Create a Workflow Job of type Quality Control
        job = conn.createJob(job_type_description=desc)
        #Get the extended properties table associated with the job
        prop_table = job.getExtendedPropertyTable('WMXADMIN.JTXX_LDM_PROPS')

        #Update value of extended property fields
        prop_table['ProjectAlias'].data=alias
        prop_table['ProjectAliasClean'].data=aliasClean
        prop_table['ProjectYear'].data=year
        prop_table['ProjectState'].data=state
        prop_table['ProjectParentDirectory'].data=parentDirectory
        prop_table['ProjectArchiveDirectory'].data=archiveDirectory
        prop_table['ProjectDeliveryDate'].data=valueHDDateRcvd
        prop_table['ProjectStartCollectionDate'].data=valueBeginDate
        prop_table['ProjectEndCollectionDate'].data=valueEndDate
        prop_table['ProjectAddQARasters'].data=r"True"
        prop_table['ProjectAddMissingRasers'].data=r"True"
        prop_table['ProjectID'].data=valueProjName
        prop_table['ProjectDirectory'].data=os.path.join(parentDirectory, valueProjName)
        prop_table['ProjectRasterFuncPath'].data=r"\\aiotxftw6na01data\SMB03\elevation\WorkflowManager\Tools\ngce\raster\ServerSide_Functions"
        prop_table['ProjectServerConPath'].data=r"\\aiotxftw6na01data\smb03\elevation\WorkflowManager\Tools\ngce\aiotxftw3gi014.usda.ags"
        prop_table['ProjectMasterName'].data=r"Master/Elevation_1M"
        prop_table['ProjectMasterPath'].data=r"\aiotxftw6na01\SMB03\elevation"


        job.save()

        job.markStepAsComplete(2885) # Start
        job.markStepAsComplete(2887) # Define AOI
        job.executeStep(2886) # Check AOI
        job.markStepAsComplete(2889) #Required props (set above)
        job.markStepAsComplete(2888) # Optional props (set above)
        job.markStepAsComplete(2908) # Job ready
        job.save()

        job.executeStep(2912) # Set Job project directory
        job.executeStep(2911)# Set job clean alias
        job.executeStep(2909)# Set job name
        job.executeStep(2910)# Set job Project ID
        job.save()

        job.executeStep(2890)# Add Job Folders
        job.executeStep(2891)# Add Job to CMDR
        job.setStepAsCurrent(2934)
        job.save()


        #Change the assignment of the job to Editors group
        job.assignedType='Group'
        job.assignedTo='Delivery'
        job.save()

        job_wmx_id = job.ID

        Deliver_ProjectID =valueProjName
        Deliver_Status = "Delivered"
        Deliver_Date = valueHDDateRcvd
        Deliver_Date_CollectionBegin=valueBeginDate
        Deliver_Date_CollectionEnd=valueEndDate
        Deliver_Notes=valueWorkflowNotes
        Deliver_Area_M=valueSqMiles*2589988.10 # Convert miles to meters
        Deliver_HorzProj=valueHorzProj
        Deliver_HorzUnit=valueHorzUnit
        Deliver_VertProj=valueVertProj
        Deliver_VertUnit=valueVertUnit
        Deliver_VertRMSE=valueRMSE
        Deliver_Exists_Boundary=("Yes" if valueBoundary == "Yes" else "No")
        Deliver_Exists_DTM="Yes"
        Deliver_DTM_CellResolution=valueCellResolu
        Deliver_DTM_PointSpacing=valueNPS
        Deliver_DTM_PointDensity=valuePtDenMeter
        Deliver_Exists_DSM = ("Yes" if valueDSMstatus=="Y_CM" else "No")
        Deliver_Link_Metadata = valueMetaXMLLin
        Deliver_Link_UserGuide=valueUserGuide
        Deliver_LAS_Classified=("Yes" if valueLasClassified=="Y" else "No")

        Deliver_Area = None
        Deliver_BoundXMin = None
        Deliver_BoundYMin = None
        Deliver_BoundXMax = None
        Deliver_BoundYMax = None
        try:
            Deliver_Area=loi.getArea("PRESERVE_SHAPE", "SQUAREMETERS")
        except:
            pass

        if Deliver_Area == None:
            Deliver_Area = Deliver_Area_M

        try:
            Deliver_BoundXMin = updatedBoundary.extent.XMin
            Deliver_BoundYMin = updatedBoundary.extent.YMin
            Deliver_BoundXMax = updatedBoundary.extent.XMax
            Deliver_BoundYMax = updatedBoundary.extent.YMax
        except:
            pass

        Utility.setWMXJobDataAsEnvironmentWorkspace(str(job_wmx_id))
        deliver = CMDR.Deliver()
        delivery = list(deliver.getDeliver(Deliver_ProjectID))
        delivery.setStatus(delivery,Deliver_Status)
        delivery.setIsLASClassified(delivery,Deliver_LAS_Classified)


        delivery.setHorzSRName(delivery,Deliver_HorzProj)
        delivery.setHorzUnit(delivery,Deliver_HorzUnit)
        delivery.setVertSRName(delivery,Deliver_VertProj)
        delivery.setVertUnit(delivery,Deliver_VertUnit)
        delivery.setPointSpacingDTM(delivery,Deliver_DTM_PointSpacing)
        delivery.setPointDensityDTM(delivery,Deliver_DTM_PointDensity)
        delivery.setBoundXMin(delivery,Deliver_BoundXMin)
        delivery.setBoundYMin(delivery,Deliver_BoundYMin)
        delivery.setBoundXMax(delivery,Deliver_BoundXMax)
        delivery.setBoundYMax(delivery,Deliver_BoundYMax)
        delivery.setDTMExists(delivery,Deliver_Exists_DTM)
        delivery.setDTMCellResolution(delivery,Deliver_DTM_CellResolution)
        delivery.setDSMExists(delivery,Deliver_Exists_DSM)
        if Deliver_Exists_DSM == 'Yes':
            delivery.setDSMCellResolution(delivery,Deliver_DTM_CellResolution)
        delivery.setDeliverDate(delivery,Deliver_Date)
        delivery.setCollectionBeginDate(delivery,Deliver_Date_CollectionBegin)
        delivery.setCollectionEndDate(delivery,Deliver_Date_CollectionEnd)
        delivery.setNotes(delivery,Deliver_Notes)
        delivery.setVertRMSE(delivery,Deliver_VertRMSE)
        delivery.setDeliverArea(delivery,Deliver_Area)

        delivery.setBoundaryExists(delivery,Deliver_Exists_Boundary)
        delivery.setLinkMetadataXML(delivery,Deliver_Link_Metadata)
        delivery.setLinkUserGuide(delivery,Deliver_Link_UserGuide)
        deliver.updateDeliver(delivery,Deliver_ProjectID)







        # break
