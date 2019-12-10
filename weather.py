#!/usr/bin/env python3

# Functional prototype for logging weather data from public API to SQL database
# an displaying history

DB_FILE = "weather.sqlite3"
LOG_FILE = "weather.log"
PERIOD = 5 # in minutes
LOCATION_NAME = "Ostrava"
LAT_LON = "49.8209,18.2625"
API_KEY = "396d860ffc2d16b44404698531c49a1e"

# load simple app server to publish script as web service
from flask import Flask, render_template
app = Flask(__name__, static_url_path='', template_folder=".")

# setup logging
import logging
logging.basicConfig(filename=LOG_FILE, format='%(levelname)s: %(asctime)s - %(message)s', \
					datefmt='%y-%b-%d %H:%M:%S')

# we'll use SQLite as database
import sqlite3
from sqlite3 import Error

import sys, os, datetime

from apscheduler.schedulers.background import BackgroundScheduler

import requests
from requests.exceptions import HTTPError

def error(msg):
	"""This function should send notifications in case of problems"""
	# just log for now
	logging.error(msg)
	
def shutdown():
	"""Gracefully end the script and Flask app server """
	raise RuntimeError("Server going down")
	
def get_data():
	"""This function gets called by the scheduler to get data from API provider and save it to the db"""
	# request from darksky
	url = f"https://api.darksky.net/forecast/{API_KEY}/{LAT_LON}?exclude=hourly,minutely,daily,alerts&units=si&lang=cs"
	try:
		response = requests.get(url)
		response.raise_for_status()
	except HTTPError as e:
		error(e)
	except Exception as e:
		error(e)
	else:
		# save to the database
		try:
			data = response.json()
			# TODO: data validation
			timestamp = data['currently']['time']
			temp = data['currently']['temperature']
			desc = data['currently']['summary']
			icon = data['currently']['icon']
			sql = f"""INSERT INTO weather_log (wl_timestamp, wl_temperature, wl_description, wl_icon)
				    VALUES ({timestamp},{temp},"{desc}","{icon}")"""
			exec_sql(sql)
		except Error as e:
			error(e)
			

def exec_sql(sql):
	"""Execute SQL command"""
	conn = sqlite3.connect(DB_FILE)
	c = conn.cursor()
	c.execute(sql)
	conn.commit()
	conn.close()
	
def fetch_sql(sql):
	"""Execute SQL command and return data"""
	conn = sqlite3.connect(DB_FILE)
	c = conn.cursor()
	c.execute(sql)
	rows = c.fetchall()
	conn.close()
	return rows

def create_db():
	sql = """ CREATE TABLE IF NOT EXISTS weather_log (
                 id integer PRIMARY KEY AUTOINCREMENT,
                 wl_timestamp datetime,
				 wl_location text,
				 wl_temperature real,
                 wl_description text,
				 wl_icon text
              ); """
	try:
		exec_sql(sql)
	except Error as e:
		error(e)
		shutdown()

def setup():
	"""Initial setup"""
	# create database if it doesn't exist
	if not os.path.isfile(DB_FILE):
		create_db()
 
	# schedule data acquisition
	scheduler = BackgroundScheduler()
	scheduler.add_job(get_data, 'cron', minute='*/'+ str(PERIOD))
	scheduler.start()

@app.template_filter('format_datetime')
def format_datetime_filter(s):
	"""Filter used in the template to format date and time"""
	return datetime.datetime.fromtimestamp(int(s)).strftime('%Y-%m-%d %H:%M:%S')

@app.route('/')
def show_all():
	"""This function gets called when an http request comes"""
	# get data
	sql = "select * from weather_log order by id desc"
	rows = fetch_sql(sql)
	if len(rows) == 0:
		return "Waiting for data, please reload in a while ..."
	else:
		# use simple template
		return render_template("index.tmpl", location = LOCATION_NAME, current = rows[0], archive = rows[1:]);

if __name__ == '__main__':
	# schedule data acquisition, etc
	setup()
	# run the app loop
	app.run()
