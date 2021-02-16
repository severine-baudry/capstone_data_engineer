import numpy as np
import pandas as pd
import configparser
import os

def parse_stations(name):
    l_res = []
    with open(name) as f:
        n=0
        for line in f:
            l_str = line.split()
            #print(l_str)
            res = l_str[0:5]
            #print(res)
            toto = " ".join(l_str[5:]) 
            #print(toto)
            res.append(toto)
            l_res.append(res)
            n+=1
#            if n ==10:
#                break
    return l_res

def NYT_counties(name):
    d_res = {}
    with open(name) as f :
        header = f.readline()
        for line in f :
            l_str = line.split(",")
            # state,county, fips
            d_res.setdefault( (l_str[2], l_str[1], l_str[3]), True  )
    return d_res

def counties_lat_long(name):
    with open(name) as f :
        headers = f.readline().split()
        l_headers = len(headers)
        print("len of headers : ", l_headers )
        l_res = []
        for line in f :
            l_str = line.split()
            state = l_str[0]
            fips = l_str[1]
            latitude = l_str[-2]
            longitude = l_str[-1]
            l = len(l_str)
            n_words = l - l_headers + 1
            county = " ".join( l_str[3:3+n_words] )
            #d_res.setdefault(state, []).append( [ state, county, fips, latitude, longitude ])
            l_res.append( [state, county, fips, latitude, longitude ])
        return l_res

class DistanceToStation :
    def __init__(self, l_stations):
        ''' init computations for latitude and longitude'''
        self.df_stations = pd.DataFrame(l_stations, columns = ["ID", "latitude_degrees", "longitude_degrees", "elevation", "stat", "name"] )
        self.df_stations["latitude_degrees"] = self.df_stations["latitude_degrees"].apply( lambda x : float(x) )
        self.df_stations["longitude_degrees"] = self.df_stations["longitude_degrees"].apply( lambda x : float(x) )
        self.df_stations["elevation"] = self.df_stations["elevation"].apply( lambda x : float(x) )

        self.df_stations["latitude"] = self.df_stations["latitude_degrees"] * np.pi / 180.
        self.df_stations["longitude"] = self.df_stations["longitude_degrees"] * np.pi / 180.
        self.df_stations["cos_latitude"] = np.cos(self.df_stations["latitude"])
           
    
    def closest_station( self, lati, longi):
        latitude = lati * np.pi / 180.
        longitude = longi * np.pi / 180.
        cos_lat = np.cos(latitude)
        # Haversine formula
        self.df_stations["delta_lat_term"] = ( np.sin( (self.df_stations["latitude"] - latitude) * 0.5 ) )**2
        self.df_stations["delta_long_term"] = ( np.sin( (self.df_stations["longitude"] - longitude) * 0.5) )**2
        self.df_stations["a"] = self.df_stations["delta_lat_term"] + self.df_stations["delta_long_term"] \
                        + cos_lat + self.df_stations["cos_latitude"]
        self.df_stations["angle"] = np.arctan2( np.sqrt(self.df_stations["a"]), np.sqrt( 1. - self.df_stations["a"] ) )
        closest = self.df_stations["angle"].idxmin()
        print(closest)
        return self.df_stations.iloc(closest)
        
                    
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("capstone.cfg")
    project_path = config["PATH"]["PROJECT"]
 
    ##### US weather stations
    l_stations = parse_stations( os.path.join(project_path, 'DATA/WEATHER/US_ghcnd_stations.txt') )
    for i,station in enumerate(l_stations):
        print(station)
        if i == 10:
            break
    print( len(l_stations))
    d_station = {}
    # 0 : ID
    # 1 : latitude
    # 2 : longitude
    # 3 : elevation
    # 4 : state
    # 5 : name
    for station in l_stations :
        d_station.setdefault(station[4], []).append(station)
#        d_station[ station[4] ].append(station)
    print(len(d_station) )
    for state, l_stt in sorted(d_station.items()):
        print(state, len(l_stt))
    
    station_distance = DistanceToStation(l_stations);
    ################################
    #### COVID deaths and case per county per day, from NewYork Times
    nytimes_counties = NYT_counties(os.path.join(project_path, "DATA/us-counties.txt") )
    print( "number of counties from NY Times:", len(nytimes_counties))
    for i,k in enumerate (nytimes_counties.keys()):
        print("\t",k)
        if i ==10:
            break
#    for county in nytimes_counties :
#        print(county)

    #######################################################
    ## counties fips to geographic coordinates (latitude, longitude) 
    gazeeter_counties= counties_lat_long( os.path.join(project_path, "DATA/2020_Gaz_counties_national.txt") )
    print("number of counties from Gazeeter : ", len(gazeeter_counties))
    for k in gazeeter_counties[:10]:
        print("\t",k)
    # 0 :state
    # 1 : county
    # 2 : fips
    # 3 : latitude
    # 4 : longitude
    # for each county, find the station closest to its 'center'
    for county in gazeeter_counties:
        closest = station_distance.closest_station( float(county[3]), float(county[4]) )
        print(county)
        print(closest)
        break
    exit()
        
    d_gazeeter = { county[2] : county for county in gazeeter_counties }
    
    map_NYT_Gazeeter = {}
    n_not_found= 0
    l_not_found = []
    for state, county, fips in nytimes_counties:
        if fips not in d_gazeeter :
            n_not_found +=1
            l_not_found.append( (state, county, fips) ) 
            print(f"fips from NY Times not found in gazeeter : {state}; {county}; {fips}")
        else :
            map_NYT_Gazeeter[ (state, county, fips) ] = d_gazeeter[fips]
        
    print("not found : ", n_not_found)
    print("found : ", len(map_NYT_Gazeeter) )
    for k in sorted(l_not_found):
        print(k)
    print("=============================")
    for k in sorted(l_not_found):
        if k[1] != "Unknown":
            print(k)

    latitude = [float(a[3]) for a in gazeeter_counties ]
    longitude = [ float(a[4]) for a in gazeeter_counties ]
    print( f'Latitude : min = {min(latitude)}, max = {max(latitude)}')
    print( f'Longitude : min = {min(longitude)}, max = {max(longitude)}')
    
                                                          
