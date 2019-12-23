#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: Jay Shah
# ASU ID: 1215102837

from pymongo import MongoClient
import os
import sys
import json
from math import cos, sin, sqrt, atan2, radians

def distance(lat1, lon1, lat2, lon2):
    
    R = 3959
    phi_1 = radians(lat1)
    phi_2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    
    a = (sin(delta_phi/2) * sin(delta_phi/2)) + (cos(phi_1) * cos(phi_2) * sin(delta_lambda/2) * sin(delta_lambda/2))
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c

    return d

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    
    all_businesses = collection.find({'city': {'$regex':cityToSearch, '$options':"$i"}})
    
    with open(saveLocation1, "w") as f:
        for business in all_businesses:
            buss_name = business['name']
            buss_addr = business['full_address'].replace("\n", ", ")
            buss_city = business['city']
            buss_state = business['state']
            
            f.write(buss_name.upper() + "$" + buss_addr.upper() + "$" + buss_city.upper() + "$" + buss_state.upper() + "\n")

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    
    all_businesses = collection.find({'categories':{'$all': categoriesToSearch}}, {'name': 1, 'latitude': 1, 'longitude': 1, 'categories': 1})
    
    lat1 = float(myLocation[0])
    lon1 = float(myLocation[1])
    
    with open(saveLocation2, "w") as f:
        for business in all_businesses:
            buss_name = business['name']
            lat2 = float(business['latitude'])
            lon2 = float(business['longitude'])
            
            calculated_distance = distance(lat1, lon1, lat2, lon2)
            if calculated_distance <= maxDistance:
                f.write(buss_name.upper() + "\n")