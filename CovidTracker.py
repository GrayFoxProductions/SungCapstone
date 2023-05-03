import urllib.request, urllib.parse, urllib.error
import http
import sqlite3
import json
import time
import ssl
import sys


ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


serviceurl ="https://data.cdc.gov/resource/9mfq-cb36.json"
serviceurl2=urllib.request.urlopen(serviceurl, context=ctx).read()


ready=json.loads(serviceurl2)

print('Welcome to the Covid Tracker Program')

while True:
    try:
        askstate=input('Enter State Abbreviation:').upper().strip()
        askyear=input('Enter Year:').strip()
        preaskmonth=input('Enter Month:').strip()
        askmonth=preaskmonth.zfill(2)

        found=False

        for line in ready:
            location=line['state']
            totalcase= int(line['tot_cases'])
            date=line['submission_date']
            if totalcase == 0: continue
            dateyear=date[0:4]
            month=date[5:7]
            if location==askstate and dateyear==askyear and month==askmonth:
                print(line)
                found=True


        if not found:
            print('No CDC Results for the state of',askstate,'in the month of',askmonth + ',','of year',askyear)

    except ValueError:
        continue





























