#
# Client-side python app for benford app, which is calling
# a set of lambda functions in AWS through API Gateway.
# The overall purpose of the app is to process a PDF and
# see if the numeric values in the PDF adhere to Benford's
# law.
#
# Authors:
#   Shreya Singh
#   Prof. Joe Hummel (initial template)
#   Northwestern University
#   CS 310
#

import requests
import jsons

import uuid
import pathlib
import logging
import sys
import os
import base64
import time

from configparser import ConfigParser


############################################################
#
# classes
#
class User:

  def __init__(self, row):
    self.userid = row[0]
    self.username = row[1]
    self.pwdhash = row[2]


class Job:

  def __init__(self, row):
    self.jobid = row[0]
    self.userid = row[1]
    self.status = row[2]
    self.originaldatafile = row[3]
    self.datafilekey = row[4]
    self.resultsfilekey = row[5]


###################################################################
#
# web_service_get
#
# When calling servers on a network, calls can randomly fail. 
# The better approach is to repeat at least N times (typically 
# N=3), and then give up after N tries.
#
def web_service_get(url):
  """
  Submits a GET request to a web service at most 3 times, since 
  web services can fail to respond e.g. to heavy user or internet 
  traffic. If the web service responds with status code 200, 400 
  or 500, we consider this a valid response and return the response.
  Otherwise we try again, at most 3 times. After 3 attempts the 
  function returns with the last response.
  
  Parameters
  ----------
  url: url for calling the web service
  
  Returns
  -------
  response received from web service
  """

  try:
    retries = 0
    
    while True:
      response = requests.get(url)
        
      if response.status_code in [200, 400, 480, 481, 482, 500]:
        #
        # we consider this a successful call and response
        #
        break;

      #
      # failed, try again?
      #
      retries = retries + 1
      if retries < 3:
        # try at most 3 times
        time.sleep(retries)
        continue
          
      #
      # if get here, we tried 3 times, we give up:
      #
      break

    return response

  except Exception as e:
    print("**ERROR**")
    logging.error("web_service_get() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return None
    

############################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number

  Parameters
  ----------
  None

  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """
  try:
    print()
    print(">> Enter a command:")
    print("   0 => end")
    print("   1 => users")
    print("   2 => jobs")
    print("   3 => reset database")
    print("   4 => upload pdf")
    print("   5 => download results")
    print("   6 => Upload and Poll")

    cmd = input()

    if cmd == "":
      cmd = -1
    elif not cmd.isnumeric():
      cmd = -1
    else:
      cmd = int(cmd)

    return cmd

  except Exception as e:
    print("**ERROR")
    print("**ERROR: invalid input")
    print("**ERROR")
    return -1


############################################################
#
# users
#
def users(baseurl):
  """
  Prints out all the users in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #
    api = '/users'
    url = baseurl + api

    # res = requests.get(url)
    res = web_service_get(url)

    #
    # let's look at what we got back:
    #
    if res.status_code == 200: #success
      pass
    else:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 500:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # deserialize and extract users:
    #
    body = res.json()

    #
    # let's map each row into a User object:
    #
    users = []
    for row in body:
      user = User(row)
      users.append(user)
    #
    # Now we can think OOP:
    #
    if len(users) == 0:
      print("no users...")
      return

    for user in users:
      print(user.userid)
      print(" ", user.username)
      print(" ", user.pwdhash)
    #
    return

  except Exception as e:
    logging.error("**ERROR: users() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# jobs
#
def jobs(baseurl):
  """
  Prints out all the jobs in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #
    api = '/jobs'
    url = baseurl + api

    # res = requests.get(url)
    res = web_service_get(url)

    #
    # let's look at what we got back:
    #
    if res.status_code == 200: #success
      pass
    else:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 500:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # deserialize and extract jobs:
    #
    body = res.json()
    #
    # let's map each row into an Job object:
    #
    jobs = []
    for row in body:
      job = Job(row)
      jobs.append(job)
    #
    # Now we can think OOP:
    #
    if len(jobs) == 0:
      print("no jobs...")
      return

    for job in jobs:
      print(job.jobid)
      print(" ", job.userid)
      print(" ", job.status)
      print(" ", job.originaldatafile)
      print(" ", job.datafilekey)
      print(" ", job.resultsfilekey)
    #
    return

  except Exception as e:
    logging.error("**ERROR: jobs() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# reset
#
def reset(baseurl):
  """
  Resets the database back to initial state.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #
    api = '/reset'
    url = baseurl + api

    res = requests.delete(url)

    #
    # let's look at what we got back:
    #
    if res.status_code == 200: #success
      pass
    else:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 500:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # deserialize and print message
    #
    body = res.json()

    msg = body

    print(msg)
    return

  except Exception as e:
    logging.error("**ERROR: reset() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# upload
#
def upload(baseurl):
  """
  Prompts the user for a local filename and user id, 
  and uploads that asset (PDF) to S3 for processing. 

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    # print("Base url is : ", baseurl)
    # obtains the filename from the user
    print("Enter PDF filename>")
    local_filename = input()

    if not pathlib.Path(local_filename).is_file():
      print("PDF file '", local_filename, "' does not exist...")
      return

    # prompts for the user id
    print("Enter user id>")
    userid = input()

    #
    # build the data packet. First step is read the PDF
    # as raw bytes:
    #
    infile = open(local_filename, "rb")
    bytes = infile.read()
    infile.close()

    #
    # now encode the pdf as base64. Note b64encode returns
    # a bytes object, not a string. So then we have to convert
    # (decode) the bytes -> string, and then we can serialize
    # the string as JSON for upload to server:
    #
    datastr = base64.b64encode(bytes).decode("utf-8")  # Encode bytes to base64, then decode to string
    # datastr = ""
    
    # TODO: data = ???
    # TODO: datastr = ???

    data = {"filename": local_filename, "data": datastr}

    #
    # call the web service:
    # using post
    url = f"{baseurl}/pdf/{userid}"
    res = requests.post(url, json=data)  # Send data in the POST request
    #
    # let's look at what we got back:
    #
    if res.status_code == 200: #success
      pass
    elif res.status_code == 400: # no such user
      print("Entered here")
      body = res.json()
      print(body)
      return
    else:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 500:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # success, extract jobid:
    #
    body = res.json()

    jobid = body

    print("PDF uploaded, job id =", jobid)
    return

  except Exception as e:
    logging.error("**ERROR: upload() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# download
#
def download(baseurl):
  """
  Prompts the user for the job id, and downloads
  that asset (PDF).

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """
  
  try:
    print("Enter job id>")
    jobid = input()
    #
    # Construct the URL for the request
    #
    url = f"{baseurl}/results/{jobid}"  # Adjust path as per your API structure
    print(f"Calling URL: {url}")

    #
    # Call the web service:
    #
    res = requests.get(url)  # Sending a GET request to the API

    
    #
    # let's look at what we got back:
    #
    if res.status_code == 200: #success
      pass
    elif res.status_code == 400: # no such job
      body = res.json()
      print(body)
      return
    elif res.status_code in [480, 481, 482]:  # uploaded
      msg = res.json()
      print("No results available yet...")
      print("Job status:", msg)
      return
    else:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 500:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return
      
    #
    # if we get here, status code was 200, so we
    # have results to deserialize and display:
    #
    #
    # Deserialize the message body
    #
    body = res.json()  # Assuming the response is in JSON format
    print(f"Response body: {body}")

    # Extract the base64-encoded data string
    datastr = body.get("data")  

    if not datastr:
        print("No data found in response.")
        return

    #
    # encode the data string to obtain the raw bytes in base64,
    # then call b64decode to obtain the original raw bytes.
    # Finally, decode() the bytes to obtain the results as a 
    # printable string.
    #
    # Decode the base64 string to get raw bytes
    base64_bytes = datastr.encode()  # Convert the base64 string to bytes
    raw_bytes = base64.b64decode(base64_bytes)  # Decode the base64 bytes to get raw bytes
    results = raw_bytes.decode("utf-8")  # Decode the raw bytes to a printable string (assuming UTF-8)
    print(results)
    return

  except Exception as e:
    logging.error("**ERROR: download() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return

import requests
import base64
import time
import random

def upload_and_poll(baseurl):
    """
    Upload a PDF and poll the server until results are ready or an error occurs.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """
    try:
        print("Enter PDF filename>")
        local_filename = input()

        if not pathlib.Path(local_filename).is_file():
            print(f"PDF file '{local_filename}' does not exist...")
            return

        print("Enter user id>")
        userid = input()

        # Read and upload the PDF
        with open(local_filename, "rb") as infile:
            file_bytes = infile.read()

        datastr = base64.b64encode(file_bytes).decode("utf-8")  # Encode file as base64 string
        data = {"filename": local_filename, "data": datastr}

        # Upload the PDF by calling the API Gateway
        upload_url = f"{baseurl}/pdf/{userid}"
        print(f"Uploading to URL: {upload_url}")
        upload_res = requests.post(upload_url, json=data)

        if upload_res.status_code != 200:
            print(f"Failed to upload. Status code: {upload_res.status_code}")
            print(f"Response: {upload_res.text}")
            return

        jobid = upload_res.json()
        print(f"Job ID: {jobid}")

        # Poll the server for results
        poll_url = f"{baseurl}/results/{jobid}"
        print(f"Polling URL: {poll_url}")

        while True:
            poll_res = requests.get(poll_url)
            status_code = poll_res.status_code

            print(f"Polling... Status code: {status_code}")

            if status_code == 200:  
                print("Results are ready!")
                try:
                    # Parse as JSON
                    body = poll_res.json()  
                    print("Parsed as json")
                    print(body)
                    if isinstance(body, dict):
                        datastr = body.get("data")
                    else:
                        print("Unexpected JSON structure, treating as raw text.")
                        datastr = poll_res.text.strip('"')  
                except ValueError:
                    datastr = poll_res.text.strip('"') 

                if not datastr:
                    print("No data found in response.")
                    return

                # Decode base64 data
                base64_bytes = datastr.encode()
                raw_bytes = base64.b64decode(base64_bytes)
                results = raw_bytes.decode("utf-8")  # Convert raw bytes to a printable string

                print("Results:")
                print(results)                
                return
            elif status_code == 400:  # No such job
                print(f"Error: {poll_res.json()}")
                return
            elif status_code in [480, 481, 482]:
                job_status = poll_res.text.strip('"')  # Extract the job status
                print("Job still in progress...")
                print(f"Job status: {poll_res.json()}")
                # Exit loop if the status indicates an error
                if "error" in job_status.lower():
                    print("Job encountered an error, exiting polling.")
                    return
            elif status_code >= 500:  # Server/network error
                print(f"Server error. Status code: {status_code}")
                print(f"Error message: {poll_res.text}")
                return
            else:
                print(f"Unexpected status code: {status_code}")
                return

            # Step 5d: Sleep for a random amount of time (1-5 seconds)
            sleep_time = random.randint(1, 5)
            print(f"Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


############################################################
# main
#
try:
  print('** Welcome to BenfordApp **')
  print()

  # eliminate traceback so we just get error message:
  sys.tracebacklimit = 0

  #
  # what config file should we use for this session?
  #
  config_file = 'benfordapp-client-config.ini'

  print("Config file to use for this session?")
  print("Press ENTER to use default, or")
  print("enter config file name>")
  s = input()

  if s == "":  # use default
    pass  # already set
  else:
    config_file = s

  #
  # does config file exist?
  #
  if not pathlib.Path(config_file).is_file():
    print("**ERROR: config file '", config_file, "' does not exist, exiting")
    sys.exit(0)

  #
  # setup base URL to web service:
  #
  configur = ConfigParser()
  configur.read(config_file)
  baseurl = configur.get('client', 'webservice')

  #
  # make sure baseurl does not end with /, if so remove:
  #
  if len(baseurl) < 16:
    print("**ERROR: baseurl '", baseurl, "' is not nearly long enough...")
    sys.exit(0)

  if baseurl == "https://YOUR_GATEWAY_API.amazonaws.com":
    print("**ERROR: update config file with your gateway endpoint")
    sys.exit(0)

  if baseurl.startswith("http:"):
    print("**ERROR: your URL starts with 'http', it should start with 'https'")
    sys.exit(0)

  lastchar = baseurl[len(baseurl) - 1]
  if lastchar == "/":
    baseurl = baseurl[:-1]

  #
  # main processing loop:
  #
  cmd = prompt()

  while cmd != 0:
    #
    if cmd == 1:
      users(baseurl)
    elif cmd == 2:
      jobs(baseurl)
    elif cmd == 3:
      reset(baseurl)
    elif cmd == 4:
      upload(baseurl)
    elif cmd == 5:
      download(baseurl)
    elif cmd == 6:
      upload_and_poll(baseurl)
    else:
      print("** Unknown command, try again...")
    #
    cmd = prompt()

  #
  # done
  #
  print()
  print('** done **')
  sys.exit(0)

except Exception as e:
  logging.error("**ERROR: main() failed:")
  logging.error(e)
  sys.exit(0)
