# load dependencies
import os
import arcpy
import csv
import numpy as np
import shutil
import datetime

# user-specified inputs
data_folder = 'C:/Users/a02046349/Downloads/Send/SendTOMaggie/WY06data' # folder holding all geodatabase folders; any non-geodatabase folders should have a '00_' prefix
cities_shp = 'C:/Users/a02046349/Downloads/Cities/Cities/US_Can_Cities_07_03_19_revised.shp' # shapefile holding cities data
output_csv_path = 'C:\\Users\\a02046349\\Test.csv' # output CSV file that will be created
buffer_distance = 5000# buffer distance in meters
coord_sys = 102005 # coordinate system for consistency and distances ('USA Contiguous Conic Equidistant' specified here.
                    # See PDF at http://desktop.arcgis.com/en/arcmap/10.6/map/projections/about-projected-coordinate-systems.htm for more options; use the EPSG number to change)


def main():

    print 'Reprojecting files and setting up folder structure.....'

    # setup arcGIS parameters
    arcpy.env.overwriteOutput = True

    # change directory to folder path provided above
    os.chdir(data_folder)

    # define output coordinate system
    outCS = arcpy.SpatialReference(coord_sys)

    # get list of all folders in directory
    gdb_list = filter(lambda x: os.path.isdir(x), os.listdir('.'))

    # recursively remove any folders starting with '00_' from list
    for dir in gdb_list[:]:
        if dir.startswith('00_'):
            gdb_list.remove(dir)

    # create folder structure for shapefiles
    out_path = os.path.join(data_folder, '00_Shapefiles') # put shapefiles in data folder under subfolder
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # create temporary directory
    temp_dir = os.path.join(data_folder, '00_Temp')
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    # table that we will be building
    table = []

    # reproject cities and create layer
    cities_proj = arcpy.Project_management(cities_shp, os.path.join(os.path.dirname(cities_shp), 'Cities_reprojected.shp'), outCS)
    cities_lyr = arcpy.MakeFeatureLayer_management(cities_proj, 'cities.lyr')

    for dir in gdb_list: # loop through each folder/gdb in list
        # print each trajectory for status tracking
        print 'Starting geodatabase:    ' + dir.split('.')[0]

        # set arcGIS workspace to geodatabase
        arcpy.env.workspace = dir

        # convert geodatabase polylines to shapefile
        arcpy.FeatureClassToShapefile_conversion("Placemarks/Polylines", out_path)

        # recursively convert geodatabase points to shapefile
        points = os.path.join(out_path, "Points.shp")
        if os.path.exists(points):
            arcpy.Delete_management(points)
        arcpy.FeatureClassToShapefile_conversion("Placemarks/Points", out_path)

        # reproject points
        reproj_points = arcpy.Project_management(points, os.path.join(out_path, "points_reproj.shp"), outCS)

        # rename shapefile as gdb folder name
        out_shp = os.path.join(out_path, 'Polylines.shp')
        new_name = os.path.join(out_path, dir).split('.')[0] + '.shp'

        # delete if renamed file already exists
        if os.path.exists(new_name):
            arcpy.Delete_management(new_name)
        arcpy.Project_management(out_shp, new_name, outCS)
        arcpy.Delete_management(out_shp)

        # find center (origin) of trajectory
        trajectory_vertices = arcpy.FeatureVerticesToPoints_management(new_name, new_name.split('.')[0] + '_points.shp')
        vertices_lyr = arcpy.MakeFeatureLayer_management(trajectory_vertices, 'vertices.lyr')
        origin_point = arcpy.Select_analysis(vertices_lyr, 'origin', "FID = 0")

        # get number of features in trajectory
        n = arcpy.GetCount_management(new_name)[0]

        # loop through for each polyline in trajectory
        for i in [4]:#range(0, int(n)):

            # print feature number for project tracking
            print '............Collecting data for feature #' + str(i)
            #try:
            # set worskpace to temp directory
            arcpy.env.workspace = temp_dir

            # select feature i
            out_feature = os.path.join(temp_dir, 'feature.shp')
            out_points= os.path.join(temp_dir, 'points.shp')
            for f in [out_feature, out_points]:
                if os.path.exists(f):
                    arcpy.Delete_management(f)
            tmp_feature = arcpy.Select_analysis(new_name, out_feature, "FID = %s" % int(i))
            tmp_points = arcpy.FeatureVerticesToPoints_management(tmp_feature, out_points)

            # buffer by specified buffer distance
            out_buffer = os.path.join(temp_dir, "buffer.shp")
            if os.path.exists(out_buffer):
                arcpy.Delete_management(out_buffer)
            arcpy.Buffer_analysis(tmp_feature, 'buffer', "%s Meters" % str(buffer_distance), "", "ROUND")

            # select cities that overlap with buffer
            city_overlap = arcpy.SelectLayerByLocation_management(cities_lyr, 'INTERSECT', out_buffer, selection_type='NEW_SELECTION')

            # extract city names from all cities intersecting buffer
            overlap_city_ids = []
            with arcpy.da.SearchCursor(city_overlap, 'FID') as cursor:
                for row in cursor:
                    overlap_city_ids.append(row[0])

            # select cities that directly intersect trajectory
            city_intersect = arcpy.SelectLayerByLocation_management(cities_lyr, 'INTERSECT', tmp_feature, selection_type='NEW_SELECTION')

            # extract city names from all cities intersecting buffer
            intersect_city_ids = []
            with arcpy.da.SearchCursor(city_intersect, 'FID') as cursor:
                for row in cursor:
                    intersect_city_ids.append(row[0])

            # remove any overlapping city ids from between the two lists
            for id in overlap_city_ids[:]:
                if id in intersect_city_ids:
                    overlap_city_ids.remove(id)

            # merge overlap and intersect cities list
            all_city_fids = intersect_city_ids + overlap_city_ids
            
            # remove selection from cities_lyr
            cities_lyr = arcpy.SelectLayerByAttribute_management(cities_lyr, selection_type='CLEAR_SELECTION')

            # loop through each intersecting city by id and calculate distance
            for city in all_city_fids:
                try:
                    calculate_distance_to_city(cities_lyr, city, tmp_feature, reproj_points, tmp_points, i, table, temp_dir, origin_point, dir, buffer_distance)
                # throw exception and continue with loop if any errors due to city
                except Exception as err:
                    print '             CALCULATING DISTANCE FAILED FOR FEATURE ' + str(i) + ' OF GEODATABASE ' + dir.split('.')[0]
                    print'              ERROR THROWN WAS: ' + str(err)

    # save table into CSV file with specified headers
    print "Saving output to " + output_csv_path
    np.savetxt(output_csv_path, table, fmt = '%s', delimiter=',', header="Trajectory Date, Site, Altitude, Feature, City FID, Distance (km), Begin Time, End Time, Speed (km/hour), Type")

    # delete temporary files
    #print "Deleting temporary files"
    #shutil.rmtree(temp_dir)
    print "-------------------SCRIPT FINISHED-------------------------"



def calculate_distance_to_city(cities_lyr, base_city_fid, feature, trajectory_points, feature_points, i, table, temp_dir, origin_point, dir, buffer_distance):
    """
    Calculates distance from feature to city with FID specified; if buffered, calculates distance from feature to buffered city
    :param cities_lyr: layer holding all cities
    :param base_city_fid: city identifier which distance is being calculated to
    :param feature: feature which distance is being calculated from
    :param trajectory_points: vertices of trajectory (all features)
    :param feature_points: feature vertices
    :param i: feature number
    :param table: table which data is being added to
    :param temp_dir: working directory for storing temp files
    :param origin_point: origin point of trajectories where distance to city is calculated from
    :param dir: folder with associated trajectory data
    :param buffer_distance: Distance in meters to use if buffering city
    """
    print '........................Calculating distance for city #' + str(base_city_fid)

    # extract that city
    out_city = os.path.join(temp_dir, "city.shp")
    if os.path.exists(out_city):
        arcpy.Delete_management(out_city)
    city_lyr = arcpy.Select_analysis(cities_lyr, 'city', "FID = %s" % int(base_city_fid))

    # get centroid of city
    city_centroid = os.path.join(temp_dir, 'city_centroid.shp')
    if os.path.exists(city_centroid):
        arcpy.Delete_management(city_centroid)
    arcpy.FeatureToPoint_management(city_lyr, city_centroid)

    # snap city centroid to trajectory
    arcpy.Snap_edit(city_centroid, [[feature, 'EDGE', '%s Meters' % int(buffer_distance + 5000)]])

    # clip trajectory by point nearest to city and select feature intersecting origin
    seg_feature = os.path.join(temp_dir, 'segmented_trajectory.shp')
    if os.path.exists(seg_feature):
        arcpy.Delete_management(seg_feature)
    arcpy.SplitLineAtPoint_management(feature, city_centroid, seg_feature)
    seg_feature_lyr = arcpy.MakeFeatureLayer_management(seg_feature, 'segmented_trajectory.lyr')
    downwind_selection = arcpy.SelectLayerByLocation_management(seg_feature_lyr, 'INTERSECT', origin_point, selection_type = 'NEW_SELECTION')# change to downwind if direction opposite
    downwind_feature = os.path.join(temp_dir, 'downwind_feature.shp')
    arcpy.CopyFeatures_management(downwind_selection, downwind_feature)
    
    # sum length of trajectory segments matching selected FIDs
    sum_len = 0 # reset at start of loop
    with arcpy.da.SearchCursor(downwind_feature, ["SHAPE@LENGTH"]) as cursor:
        for row in cursor:
            sum_len += row[0]

    # find end time from nearest point to city
    points_merge = arcpy.SpatialJoin_analysis(feature_points, trajectory_points, "spatial_join_points", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_COMMON", match_option = "INTERSECT")
    point_city_join = arcpy.SpatialJoin_analysis(city_lyr, points_merge, "point_city_join", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_COMMON", match_option="CLOSEST")
    with arcpy.da.SearchCursor(point_city_join, ['EndTime_1']) as cursor:
        for row in cursor:
            begin_time = row[0]
            
    # find beginning time
    end_times = []
    with arcpy.da.SearchCursor(points_merge, ['FID', 'EndTime_1']) as cursor:
        for row in cursor:
            if row[1] == ' ':
                pass
            else:
                end_times.append(row[1])
    end_times.sort(reverse=True)
    end_time = end_times[0]

    # calculate speed: distance / time
    if begin_time == ' ':
        speed = 'None - at origin'
    else:
        begin = datetime.datetime.strptime(str(begin_time), '%Y-%m-%d %H:%M:%S')
        end = datetime.datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S')
        time = end - begin
        time_in_hours = time.total_seconds()/3600
        speed = (sum_len/1000) / time_in_hours

    # enter lines in CSV file with trajectory, height, city name, and distance from above
    traj_first = dir.split('.')[0].split('_')[1]
    traj_date = traj_first[0:6]
    traj_site = dir[0:4]
    traj_altitude = traj_first[6:]
    table.append([traj_date, traj_site, traj_altitude, i, base_city_fid, sum_len/1000, begin_time, end_time, speed])

    # clear all data from this loop
    del city_lyr, seg_feature_lyr, seg_feature, downwind_feature, begin_time, end_time, speed, traj_first, traj_date, traj_site, traj_altitude

    """# buffer city by specified distance
    if buffer_city and buffer_distance is not None:
        out_buffer = os.path.join(temp_dir, "city_buffer.shp")
        if os.path.exists(out_buffer):
            arcpy.Delete_management(out_buffer)
        city_buffer = arcpy.Buffer_analysis(city_lyr, 'city_buffer', "%s Meters" % str(buffer_distance), "", "ROUND")

    # segment where trajectory intersects buffer around city & convert to layer
    if buffer_city:
        seg_feature = segment_by_city(feature, city_buffer, temp_dir)
    else:
        seg_feature = segment_by_city(feature, city_lyr, temp_dir)
    seg_feature_lyr = arcpy.MakeFeatureLayer_management(seg_feature, 'segmented_by_city.lyr')

    # select trajectory segment that intersects with origin & get FID of this segment
    downwind_feature = arcpy.SelectLayerByLocation_management(seg_feature_lyr, 'INTERSECT', origin_point, selection_type = 'NEW_SELECTION')# change to downwind if direction opposite
    #downwind feature = arcpy.SelectLayerByAttribute_management(seg_feature_lyr, selection_type = 'SWITCH_SELECTION')
    downwind_output = os.path.join(temp_dir, 'downwind_feature.shp')
    if os.path.exists(downwind_output):
        arcpy.Delete_management(downwind_output)
    arcpy.CopyFeatures_management(downwind_feature, downwind_output)
    downwind_fid = []
    with arcpy.da.SearchCursor(downwind_feature, ['FID']) as cursor:
        for row in cursor:
            downwind_fid = row[0]

    # select trajectory endpoint that is closest to city (or city buffer) & get FID of this segment
    if buffer_city:
        city_feature = arcpy.SelectLayerByLocation_management(seg_feature_lyr, 'INTERSECT', out_buffer, selection_type = 'NEW_SELECTION')
    else:
        city_feature = arcpy.SelectLayerByLocation_management(seg_feature_lyr, 'INTERSECT', out_city, selection_type = 'NEW_SELECTION')
    city_output = os.path.join(temp_dir, 'city_feature.shp')
    if os.path.exists(city_output):
        arcpy.Delete_management(city_output)
    arcpy.CopyFeatures_management(city_feature, city_output)
    city_fid = []
    with arcpy.da.SearchCursor(city_feature, ['FID']) as cursor:
        for row in cursor:
            city_fid = row[0]

    # define FIDs between city and origin
    fids = []
    if city_fid > downwind_fid:
        fids = range(downwind_fid, city_fid - 1)
    elif city_fid < downwind_fid:
        fids = range(city_fid + 1, downwind_fid)
    else:
        fids = [downwind_fid]

    # get point nearest to city
    point_city_join = arcpy.SpatialJoin_analysis(city_lyr, points_merge, "point_city_join", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_COMMON", match_option="CLOSEST")
    # get FID of joined point
    with arcpy.da.SearchCursor(point_city_join, ["TARGET_FID"]) as cursor:
        for row in cursor:
            target_fid = row[0]
    # select point with above FID
    nearest_point =  os.path.join(temp_dir, 'nearest_point_to_city.shp')
    if os.path.exists(nearest_point):
        arcpy.Delete_management(nearest_point)
    points_lyr = arcpy.MakeFeatureLayer_management(points_merge, "points.lyr")
    target_point = arcpy.SelectLayerByAttribute_management(points_lyr, 'NEW_SELECTION', "FID = %s" % int(target_fid))
    arcpy.CopyFeatures_management(target_point, nearest_point)



def segment_by_city(trajectory, cities, temp_dir):
    
    #Segments the selected feature where it intersects with cities, and outputs segmented trajectory
    #:param trajectory: Path to the network that we want to segment
    #:param cities: The shape file we use to segment
    #:return: segmented network
    

    # name output; remove if already present to avoid using old output
    temp_segmented = os.path.join(temp_dir, "temp_segmented_traj.shp")
    if os.path.exists(temp_segmented):
        arcpy.Delete_management(temp_segmented)

    # copy cities data to temp shapefile for editing
    cities_temp = os.path.join(temp_dir, 'Cities_seg.shp')
    arcpy.CopyFeatures_management(cities, cities_temp)

    # copy trajectory data to output shapefile for editing
    arcpy.CopyFeatures_management(trajectory, temp_segmented)

    # add segmentation by cities within buffer limit
    arcpy.FeatureToLine_management([trajectory, cities], temp_segmented)

    return temp_segmented
"""
    

if __name__ == "__main__":
    main()
