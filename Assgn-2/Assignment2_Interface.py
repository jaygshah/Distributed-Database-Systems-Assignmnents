#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
	con = openconnection
	cur = con.cursor()
	
	rgprefix = "RangeRatingsPart"
	cur.execute("select * from RangeRatingsMetadata;")
	metarows = cur.fetchall()
	for row in metarows:
		minRating = row[1]
		maxRating = row[2]
		tableName = rgprefix + str(row[0])
		
		if not ((ratingMinValue > maxRating) or (ratingMaxValue < minRating)):
			cur.execute("select * from " + tableName + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
			results = cur.fetchall()
			
			with open(outputPath, "a") as file:
				for result in results:
					file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")
	
	rbnprefix = "RoundRobinRatingsPart"
	cur.execute("select PartitionNum from RoundRobinRatingsMetadata;")
	numPartitions = cur.fetchall()[0][0]
	for i in range(0, numPartitions):
		tableName = rbnprefix + str(i)
		cur.execute("select * from " + tableName + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
		results = cur.fetchall()
		
		with open(outputPath, "a") as file:
			for result in results:
				file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")

def PointQuery(ratingValue, openconnection, outputPath):
	con = openconnection
	cur = con.cursor()
	
	rgprefix = "RangeRatingsPart"
	cur.execute("select * from RangeRatingsMetadata;")
	metarows = cur.fetchall()
	for row in metarows:
		minRating = row[1]
		maxRating = row[2]
		tableName = rgprefix + str(row[0])
		
		if ((row[0] == 0 and ratingValue >= minRating and ratingValue <= maxRating) or (row[0] != 0 and ratingValue > minRating and ratingValue <= maxRating)):
			cur.execute("select * from " + tableName + " where rating = " + str(ratingValue) + ";")
			results = cur.fetchall()
			
			with open(outputPath, "a") as file:
				for result in results:
					file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")
	
	rbnprefix = "RoundRobinRatingsPart"
	cur.execute("select PartitionNum from RoundRobinRatingsMetadata;")
	numPartitions = cur.fetchall()[0][0]
	for i in range(0, numPartitions):
		tableName = rbnprefix + str(i)
		cur.execute("select * from " + tableName + " where rating = " + str(ratingValue) + ";")
		results = cur.fetchall()
		
		with open(outputPath, "a") as file:
			for result in results:
				file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")