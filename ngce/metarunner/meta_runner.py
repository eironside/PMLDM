from math import sqrt
import datetime
import requests
import logging
import arcpy
import json
import time
import sys
import os

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_config(in_file):

    with open(in_file) as config:
        param_dict = json.load(config)

    return param_dict


def get_logger(t_dir, s_time):

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Debug Handler for Console Checks - logger.debug(msg)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    # Log Handler for Reports - logger.info(msg)
    log_handler = logging.FileHandler(os.path.join(t_dir, 'run_log.txt'), 'w')
    log_handler.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    logger.info('Script Started: {}\n'.format(
        datetime.datetime.fromtimestamp(s_time).strftime('%Y-%m-%d %H:%M:%S')
    ))

    print "Created logger {}".format(logger)
    return logger


def get_gdbs(base):

    logger.info('Collecting Project Databases')

    r_dirs = []

    if os.path.exists(base):

        sub_a = [
            d for d in os.listdir(base)
            if os.path.isdir(os.path.join(base, d))
        ]

        for a_dir in sub_a:

            try:
                if 'DERIVED' in os.listdir(os.path.join(base, a_dir)):
                    expected = (os.path.join(base, a_dir, 'DERIVED', a_dir + '.gdb'))
                    if os.path.exists(expected):
                        r_dirs.append(expected)

                else:
                    sub_b = [
                        d for d in os.listdir(os.path.join(base, a_dir))
                        if os.path.isdir(os.path.join(os.path.join(base, a_dir), d))
                    ]

                    for b_dir in sub_b:
                        if 'DERIVED' in os.listdir(os.path.join(base, a_dir, b_dir)):
                            expected = (os.path.join(base, a_dir, 'DERIVED', a_dir + '.gdb'))
                            if os.path.exists(expected):
                                r_dirs.append(expected)
                                
            except Exception as e:
                logger.debug('Exception: {}'.format(str(e)))

    else:
        raise Exception('Base Directory {} Not Found'.format(base))

    logger.info('{} FGDBs Found For Processing'.format(len(r_dirs)))
    return r_dirs


def handle_BoundaryRaster(c_target, f_target, m_dict, c_dict):

    for el_type, pairs in m_dict:

        query = """{0}='{1}'""".format('el_type', el_type)
        feat_fields = [key for key in pairs.keys() if key != 'SHAPE@']
        cmdr_fields = [val for val in pairs.values() if val != 'geometry']

        if arcpy.Exists(f_target):
            with arcpy.da.SearchCursor(f_target, list(feat_fields), query) as cursor:
                for row in cursor:
                    for cmdr_field, data in zip(cmdr_fields, list(row)):
                        c_dict.update({cmdr_field: data})

            if 'SHAPE@' in pairs.keys():
                with arcpy.da.SearchCursor(f_target, ['SHAPE@'], query) as cursor:
                    for row in cursor:
                        poly = row[0]
                        poly_gcs = poly.projectAs(arcpy.SpatialReference(4326))
                        c_dict.update({c_target + '_Extent_MinLon': poly_gcs.extent.XMin})
                        c_dict.update({c_target + '_Extent_MinLat': poly_gcs.extent.YMin})
                        c_dict.update({c_target + '_Extent_MaxLon': poly_gcs.extent.XMax})
                        c_dict.update({c_target + '_Extent_MaxLat': poly_gcs.extent.YMax})
                        c_dict.update(({'geometry': json.loads(poly_gcs.JSON)}))


def handle_BoundaryLASDataset(f_target, m_dict, c_dict):

    # Check If Any Configured Fields Are Not In Feature Class
    target_fields = [f.name for f in arcpy.ListFields(f_target)]
    m_dict = {key: value for key, value in m_dict.items() if key in target_fields}

    # Read Values From FC Into Insertion Dictionary
    feat_fields = m_dict.keys()
    cmdr_fields = m_dict.values()
    with arcpy.da.SearchCursor(f_target, list(feat_fields)) as cursor:
        for row in cursor:
            for cmdr_field, data in zip(cmdr_fields, list(row)):
                c_dict.update({cmdr_field: data})


def handle_Rasters(r_target, m_dict, c_dict):

    rast_props = list(m_dict.keys())
    cmdr_fields = list(m_dict.values())

    if arcpy.Exists(r_target):
        raster = arcpy.Raster(r_target)
        for prop, field in zip(rast_props, cmdr_fields):

            if prop == 'm_sqrt':
                value = sqrt(raster.mean)
                c_dict.update({field: value})

            else:
                value = raster.__getattribute__(prop)
                c_dict.update({field: value})


def handle_Contract(r_string):

    # r_string = DTM or DSM

    r_dir = (os.path.join(os.path.abspath(os.path.join(db, "../..")), 'DELIVERED', r_string[-3:]))
    if os.path.exists(r_dir):
        arcpy.env.workspace = r_dir
        cmdr_dict.update({r_string: len(arcpy.ListRasters())})
    else:
        logger.info('Directory {} Was Not Found for Contract Processing'.format(r_string[-3:]))
        cmdr_dict.update({r_string: 0})
        


def handle_QC(c_dict):

    dtm_count = c_dict.get("QC_DTM_Count_Raster", 0)
    if dtm_count:
        c_dict.update({"QC_Exists_DTM": "True"})
    else:
        c_dict.update({"QC_Exists_DTM": "False"})

    dsm_count = c_dict.get("QC_DSM_Count_Raster", 0)
    if dsm_count:
        c_dict.update({"QC_Exists_DSM": "True"})
    else:
        c_dict.update({"QC_Exists_DSM": "False"})

    try:
        c_dict.update({"QC_Count_Raster": sum([dtm_count, dsm_count])})
    except:
        logger.info('Could Not Get Sum of DTM & DSM Counts for Project')

    c_02 = c_dict.get("c2_ptct", 0)
    c_08 = c_dict.get("c8_ptct", 0)
    try:
        c_dict.update({"QC_DTM_Count_Point": sum([c_02, c_08])})
        c_dict.pop("c2_ptct", None)
        c_dict.pop("c8_ptct", None)
    except:
        logger.info('Could Not Get Sum For QC_DTM_Count_Point')
        c_dict.pop("c2_ptct", None)
        c_dict.pop("c8_ptct", None)


def handle_Deliver(c_dict):

    dtm_count = c_dict.get("Deliver_DTM_Count_Raster", 0)
    if dtm_count:
        c_dict.update({"Deliver_Exists_DTM": "True"})
    else:
        c_dict.update({"Deliver_Exists_DTM": "False"})

    dsm_count = c_dict.get("Deliver_DSM_Count_Raster", 0)
    if dsm_count:
        c_dict.update({"Deliver_Exists_DSM": "True"})
    else:
        c_dict.update({"Deliver_Exists_DSM": "False"})

    try:
        c_dict.update({"Deliver_Count_Raster": sum([dtm_count, dsm_count])})
    except:
        logger.info('Could Not Get Sum of DTM & DSM Counts for Project')

    c_02 = c_dict.get("c2_ptct", 0)
    c_08 = c_dict.get("c8_ptct", 0)
    try:
        c_dict.update({"Deliver_DTM_PointCount": sum([c_02, c_08])})
        c_dict.pop("c2_ptct", None)
        c_dict.pop("c8_ptct", None)
    except:
        logger.info('Could Not Get Sum For Deliver_DTM_PointCount')
        c_dict.pop("c2_ptct", None)
        c_dict.pop("c8_ptct", None)


def handle_unit_conversion(c_target, c_dict, val):

    conversion_keys = [
        'DTM_CellResolution',
        'DTM_PointDensity',
        'DTM_PointSpacing',
        'DTM_PulseDensity',
        'DTM_PulseSpacing',
        'DSM_CellResolution',
        'DSM_PointDensity',
        'DSM_PointSpacing',
        'DSM_PulseDensity',
        'DSM_PulseSpacing'
    ]

    for con_key in conversion_keys:

        match_key = '_'.join([c_target, con_key])

        if match_key in c_dict.keys():
            ft_val = float(c_dict.get(match_key)) * val
            c_dict.update({match_key: ft_val})


def get_token(username, password, base_url):

    payload = {
        'username': username,
        'password': password,
        'client': 'requestip',
        'expiration': 120,
        'f': 'json'
    }
    
    token_url = base_url + '/generateToken'

    try:
        r = requests.post(token_url, data=payload, verify=False)
        
        if r.status_code != 200:
            logger.info('Server Did Not Return 200 When Requesting Token: {}'.format(r.content))
            raise Exception('Unable To Acquire Credentials from ArcGIS Server')

        else:
            if 'error' in r.json():
                logger.info(r.json()['error']['details'])
                raise Exception('Error Encountered When Collecting Token')
                
            else:
                return r.json()['token']
            
    except IOError as e:
        logger.info('Please Ensure URL to Portal is Valid: ', e.__dict__)
        return


def handle_rest_insertion(project_id, c_target, c_dict, feat_url, token):

    # Point Updates To Appropriate Layer
    if c_target == 'QC':
        svc_url = '{}/1'.format(feat_url)
    elif c_target == 'Deliver':
        svc_url = '{}/2'.format(feat_url)
    elif c_target == 'Contract':
        svc_url = '{}/3'.format(feat_url)
    else:
        raise Exception('Rest Insertion Target Not Defined: {}'.format(c_target))

    object_id = query_existing_records(project_id, svc_url, token)

    if object_id:
        logger.info('Feature Found With Object ID: {}'.format(object_id))
        logger.info('Updating Record . . .')
        status = update_record(svc_url, token, c_dict, object_id)
        
    else:
        logger.info('No Feature Found With Project ID: {}'.format(project_id))
        logger.info('Creating New Record . . .')
        status = add_record(svc_url, token, c_dict)
    
    if status:
        logger.info('Success: {}'.format(c_target))
        
    else:
        logger.info('Failure: {}'.format(c_target))


def query_existing_records(project_id, svc_url, token):

    payload = {
        'where': "Project_ID='" + project_id + "'",
        'token': token,
        'outFields': '*',
        'f': 'json'
    }
    query_url = '{0}/{1}'.format(svc_url, 'query')
    r = requests.get(query_url, params=payload, verify=False)
    response = r.json()

    if response['features']:
        return response['features'][0]['attributes']['OBJECTID']
    else:
        return None


def update_record(svc_url, token, c_dict, object_id):

    c_dict.update({'OBJECTID': object_id})

    update = [{'attributes': c_dict,}]

    payload = {
        'features': json.dumps(update),
        'token': token,
        'f': 'json'
    }
    update_url = '{}/{}'.format(svc_url, 'updateFeatures')
    r = requests.post(update_url, data=payload, verify=False)

    if r.status_code == 200:
        response = r.json()
        if 'updateResults' in response:
            if 'success' in response.get('updateResults')[0]:
                return True
            else:
                return False
    else:
        return False


def add_record(svc_url, token, insert_dict):

    geometry = insert_dict.get('geometry', None)

    if geometry:
        insert_dict.pop('geometry', None)
        update = [{'attributes': insert_dict, 'geometry': geometry}]

    else:
        update = [{'attributes': insert_dict}]

    payload = {
        'features': json.dumps(update),
        'token': token,
        'f': 'json'
    }
    add_url = '{0}/{1}'.format(svc_url, 'addFeatures')
    r = requests.post(add_url, data=payload, verify=False)

    if r.status_code == 200:
        if 'addResults' in r.json():
            try:
                logger.info(r.json())
                message = r.json().get('addResults')[0]
                if 'success' in message:
                    return True
                else:
                    return False
            except:
                return False

    else:
        logger.info('Record Insertion Did Not Return 200 Status: {}'.format(r.status_code))
        return False


if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # Get Logger
    logger = get_logger(this_dir, start_time)

    try:
        # Get Configuration Parameters From Script Directory
        config_path = os.path.join(this_dir, 'config.json')
        if not os.path.exists(config_path):
            config_path = os.path.join(this_dir,'ngce','metarunner','config.json')    
        params = get_config(config_path)
        base_dir = params['base_dir']
        base_url = params['base_url']
        username = params['username']
        password = params['password']
        feat_url = params['feat_url']
        features = params['features']

        # Collect FGDB Paths For Processing
        gdbs = get_gdbs(base_dir)

        # Collect Token For REST Insertions
        token = get_token(username, password, base_url)

        # Process Configured CMDR Targets for Each Project DB Found
        for db in gdbs:

            # Collect Project ID as DB Name
            project_id = os.path.split(db)[1].split('.')[0]

            try:
                # CMDR_Target = QC/Contract/Deliver & Mapping_Dict = FC/Raster Mappings
                for cmdr_target, mapping_dict in features.items():

                    logger.info('{}'.format('*' * 50))
                    logger.info('Processing {} for {}'.format(cmdr_target, project_id))
                    logger.info('{}'.format('*' * 50))

                    # Insertion Dictionary
                    cmdr_dict = {"Project_ID": project_id}

                    for db_ob, mapping in mapping_dict.items():

                        # Check If db_ob Is In Expected Raster Object List & Handle Accordingly
                        if db_ob not in [
                            'POINT_COUNT_LAST',
                            'PULSE_COUNT_LAST',
                            'POINT_COUNT_FIRST',
                            'PULSE_COUNT_FIRST'
                        ]:

                            # Set Path To Feature Class & Extract Values Into CMDR Dictionary
                            feat_target = os.path.join(db, db_ob)

                            if not arcpy.Exists(feat_target):
                                logger.info('Feature {} Does Not Exist'.format(db_ob))
                                continue

                            if db_ob == "BoundaryRaster":
                                try:
                                    handle_BoundaryRaster(cmdr_target, feat_target, mapping.items(), cmdr_dict)
                                except Exception as e:
                                    logger.info('Exception While Handling Boundary Raster: {}'.format(str(e)))

                            elif db_ob == "BoundaryLASDataset":
                                try:
                                    handle_BoundaryLASDataset(feat_target, mapping, cmdr_dict)
                                except Exception as e:
                                    logger.info('Exception While Handling BoundaryLASDataset: {}'.format(str(e)))

                            else:
                                logger.info('Missing Handler For Configured Target: {}'.format(db_ob))
                                continue

                            # Ensure Values Are Set to Meters
                            unit = arcpy.Describe(feat_target).spatialReference.linearUnitName
                            if unit != 'Meter':
                                handle_unit_conversion(cmdr_target, cmdr_dict, 0.3048)

                        else:
                            # Set Path To Raster & Extract Values Into cmdr_dict
                            rast_target = os.path.join(db, db_ob)

                            if not arcpy.Exists(rast_target):
                                logger.info('Raster {} Does Not Exist for {}'.format(db_ob, project_id))
                                continue

                            handle_Rasters(rast_target, mapping, cmdr_dict)

                            # Ensure Values Are Set to Meters
                            unit = arcpy.Describe(rast_target).spatialReference.linearUnitName
                            if unit != 'Meter':
                                handle_unit_conversion(cmdr_target, cmdr_dict, 0.3048)

                    # Handle Logic Specific To Each DB Object
                    if cmdr_target == "Contract":
                        for raster in ['Contract_DTM', 'Contract_DSM']:
                            handle_Contract(raster)

                    if cmdr_target == "QC":
                        handle_QC(cmdr_dict)

                    if cmdr_target == "Deliver":
                        handle_Deliver(cmdr_dict)

                    # Insert Values Into Feature Service
                    handle_rest_insertion(project_id, cmdr_target, cmdr_dict, feat_url, token)

            except Exception as exc:
                logger.info('Processing Failed For Project: {}'.format(project_id))
                logger.info('Line: {}'.format(str(sys.exc_info()[2].tb_lineno)))
                logger.info('Exception: {}'.format(str(exc)))

    except Exception as exc:
        logger.info('General Exception Raised While Executing Program')
        logger.info('Line: {}'.format(str(sys.exc_info()[2].tb_lineno)))
        logger.info('Exception: {}'.format(str(exc)))

    finally:
        logger.info('Run Length: {}'.format(time.time() - start_time))
        for handler in logger.handlers:
            handler.close()
