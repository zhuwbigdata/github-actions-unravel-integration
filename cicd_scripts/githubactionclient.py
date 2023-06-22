# %%
import sys
from datetime import timedelta, datetime
import json
import re
import getopt
import urllib3
import requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# %%
# DBRKS URL pattern
pattern = r"^https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)$"
pattern_as_text = r"https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)"  
cleanRe = re.compile('<.*?>')

app_summary_map = {}
app_summary_map_list = []

# %%
def get_api(api_url, api_token):
    response = requests.get(api_url,
                            verify=False,
                            headers={'Authorization': api_token})
    json_obj = json.loads(response.content)
    return json_obj

def check_response_on_get(json_val):
    if 'message' in json_val :
        if json_val['message'] == 'INVALID_TOKEN' :
           raise ValueError('INVALID_TOKEN')

def post_api(api_url, api_token, kv_dict):
    response = requests.post(api_url,
                             data = json.dumps(kv_dict),
                             verify=False,
                             headers={'Authorization': api_token,
                                      'accept': 'application/json',
                                      'Content-Type': 'application/json'})
    json_obj = json.loads(response.content)
    return json_obj

def check_response_on_post(json_val):
    if 'message' in json_val :
        if json_val['message'] == 'INVALID_TOKEN' :
           raise ValueError('INVALID_TOKEN')
    elif len(json_val) == 0:
        print(json_val)
        raise ValueError('Response is empty') 
    elif 'results' not in json_val:
        print(json_val)
        raise ValueError('KEY results NOT FOUND')

# %%
def search_by_job_id(base_url, api_token, start_time, end_time, job_id):
    api_url = base_url + \
              '/api/v1/ds/api/v1/databricks/runs'
    kv_pairs = {"from":0,
                "appTypes":["db"],
                "appStatus":["K","F","R","S","P","U","W"],
                "size":"1000",
                "start_time":start_time,
                "end_time":end_time,
                "jobIds": [ job_id ],
                "sort": [{"startTime": {"order": "desc"}}],
                "queryOnFinishedTime": False,
                }
    print("URL: " + api_url)
    print(kv_pairs)
    json_val  = post_api(api_url, api_token, kv_pairs)
    check_response_on_post(json_val)
    return json_val

# %%
def search_by_job_id(base_url, api_token, start_time, end_time, job_id):
    api_url = base_url + \
              '/api/v1/ds/api/v1/databricks/runs'
    kv_pairs = {"from":0,
                "appTypes":["db"],
                "appStatus":["K","F","R","S","P","U","W"],
                "size":"1000",
                "start_time":start_time,
                "end_time":end_time,
                "jobIds": [ job_id ],
                "sort": [{"startTime": {"order": "desc"}}],
                "queryOnFinishedTime": False,
                }
    print("URL: " + api_url)
    print(kv_pairs)
    json_val  = post_api(api_url, api_token, kv_pairs)
    check_response_on_post(json_val)
    return json_val

# %%
def search_by_globalsearchpattern(base_url, api_token, start_time, end_time, gsp):
    api_url = base_url + \
              '/api/v1/ds/api/v1/databricks/runs'
    kv_pairs = {"from":0,
                "appTypes":["db"],
                "appStatus":["K","F","R","S","P","U","W"],
                "size":"1",
                "start_time":start_time,
                "end_time":end_time,
                "globalsearchpattern": gsp,
                "sort": [{"startTime": {"order": "desc"}}],
                "queryOnFinishedTime": False,
                }
    print("URL: " + api_url)
    print(kv_pairs)
    json_val  = post_api(api_url, api_token, kv_pairs)
    check_response_on_post(json_val)
    return json_val

# %%
def search_summary_by_globalsearchpattern(base_url, api_token, start_time, end_time, gsp):
    api_url = base_url + \
              '/api/v1/ds/api/v1/databricks/runs/' + gsp + '/tasks/summary'
    print("URL: " + api_url)
    json_val  = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val

# %%
def search_analysis(base_url, api_token, clusterUId, id):
    api_url = base_url + '/api/v1/spark/' + clusterUId + '/' + id + '/analysis'
    print("URL: " + api_url)
    json_val  = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val

# %%
def search_summary(base_url, api_token, clusterUId, id):
    api_url = base_url + "/api/v1/spark/" + clusterUId + "/" + id + "/appsummary"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val

# %%
def getCurrentDateTime():   
    today = datetime.today()
    return  datetime(year=today.year, month=today.month, day=today.day, hour=today.hour, second=today.second) 

# %%
def get_job_runs_from_description(pr_id, description_json):
  job_run_list = []
  for run_url in description_json['runs']:
      match = re.search(pattern, run_url)
      if match:
        print(run_url)
        workspace_id = match.group(3)
        job_id = match.group(4)
        run_id = match.group(5)
        job_run_list.append({'pr_id': pr_id, 'pdbrks_url': run_url, 'workspace_id': workspace_id, 'job_id': job_id, 'run_id': run_id})
  
  return job_run_list

# %%
def get_job_runs_from_description_as_text(pr_id, description_text):
  job_run_list = []
  print("Description:\n" + description_text)
  print("Patten: " + pattern_as_text)
  matches = re.findall(pattern_as_text, description_text)
  if matches:
    for match in matches:
      workspace_id = match[2]
      job_id = match[3]
      run_id = match[4]
      job_run_list.append({'pr_id': pr_id, 'workspace_id': workspace_id, 'job_id': job_id, 'run_id': run_id})
  else:
    print("no match")
  return job_run_list

# %%
def create_comments_with_markdown(job_run_result_list):
    comments = ""
    if job_run_result_list:
        for r in job_run_result_list:
            comments += "----\n"
            comments += "<details>\n"
            # comments += "<img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'>\n\n"
            comments += "<summary> <img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'> Job Id: {}, Run Id: {}</summary>\n\n".format(
                r["job_id"], r["run_id"]
            )
            comments += "#### Workspace Id:" + r["workspace_id"] + "\n"
            comments += "#### Job Id:" + r["job_id"] + "\n"
            comments += "#### Run Id:" + r["run_id"] + "\n"
            comments += "----\n"
            comments += "#### [{}]({})\n".format('Unravel url', r["unravel_url"])
            if r['app_summary']:
                # Get all unique keys from the dictionaries while preserving the order
                headers = []
                for key in r['app_summary'].keys():
                    if key not in headers:
                        headers.append(key)

                # Generate the header row
                header_row = "| " + " | ".join(headers) + " |"

                # Generate the separator row
                separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

                # Generate the data rows
                data_rows = "\n".join(
                    [
                        "| " + " | ".join(str(r['app_summary'].get(h, "")) for h in headers)
                    ]
                )

                # Combine the header, separator, and data rows
                comments += "----\n"
                comments += "# App Summary\n"
                comments += "----\n"
                comments += header_row + "\n" + separator_row + "\n" + data_rows + "\n"
            if r["unravel_insights"]:
                comments += "----\n"
                comments += "## Unravel Insights\n"
                for insight in r["unravel_insights"]:
                    categories = insight["categories"]
                    if categories:
                        for k in categories.keys():
                            instances = categories[k]["instances"]
                            if instances:
                                for i in instances:
                                    if i["key"].upper() != "SPARKAPPTIMEREPORT":
                                        comments += (
                                            "#### "
                                            + i["key"].upper()
                                            + ": "
                                            + i["title"]
                                            + "\n"
                                        )
                                        comments += "##### EVENT: " + i["events"] + "\n"
                                        comments += (
                                            "##### ACTIONS: " + i["actions"] + "\n"
                                        )
            comments += "</details>\n\n"

    return comments

# %%
def fetch_app_summary(unravel_url, unravel_token, clusterUId, appId):
    app_summary_map = {}
    autoscale_dict = {}
    summary_dict = search_summary(unravel_url, unravel_token, clusterUId, appId)
    summary_dict = summary_dict["annotation"]
    url = '{}/#/app/application/spark?execId={}&clusterUid={}'.format(unravel_url,appId,clusterUId)
    app_summary_map["Spark App"] = '[{}]({})'.format(appId, url)
    app_summary_map["Cluster"] = clusterUId
    app_summary_map["Total cost"] = '${}'.format(summary_dict["cents"] + summary_dict["dbuCost"])
    runinfo = json.loads(summary_dict["runInfo"])
    app_summary_map["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map["Driver Node Type"] = runinfo["driver_node_type_id"]
    app_summary_map["Tags"] = runinfo["default_tags"]
    if 'custom_tags' in runinfo.keys():
        app_summary_map["Tags"] = {**app_summary_map["Tags"], **runinfo["default_tags"]}
    if "autoscale" in runinfo.keys():
        autoscale_dict["autoscale_min_workers"] = runinfo["autoscale"]["min_workers"]
        autoscale_dict["autoscale_max_workers"] = runinfo["autoscale"]["max_workers"]
        autoscale_dict["autoscale_target_workers"] = runinfo["autoscale"][
            "target_workers"
        ]
        app_summary_map['Autoscale'] = autoscale_dict
    else:
        app_summary_map['Autoscale'] = 'Autoscale is not enabled.'
    return app_summary_map

# %%
def fetch_app_summary(unravel_url, unravel_token, clusterUId, appId):
    app_summary_map = {}
    autoscale_dict = {}
    summary_dict = search_summary(unravel_url, unravel_token, clusterUId, appId)
    summary_dict = summary_dict["annotation"]
    url = '{}/#/app/application/spark?execId={}&clusterUid={}'.format(unravel_url,appId,clusterUId)
    app_summary_map["Spark App"] = '[{}]({})'.format(appId, url)
    app_summary_map["Cluster"] = clusterUId
    app_summary_map["Total cost"] = '${}'.format(summary_dict["cents"] + summary_dict["dbuCost"])
    runinfo = json.loads(summary_dict["runInfo"])
    app_summary_map["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map["Driver Node Type"] = runinfo["driver_node_type_id"]
    app_summary_map["Tags"] = runinfo["default_tags"]
    if 'custom_tags' in runinfo.keys():
        app_summary_map["Tags"] = {**app_summary_map["Tags"], **runinfo["default_tags"]}
    if "autoscale" in runinfo.keys():
        autoscale_dict["autoscale_min_workers"] = runinfo["autoscale"]["min_workers"]
        autoscale_dict["autoscale_max_workers"] = runinfo["autoscale"]["max_workers"]
        autoscale_dict["autoscale_target_workers"] = runinfo["autoscale"][
            "target_workers"
        ]
        app_summary_map['Autoscale'] = autoscale_dict
    else:
        app_summary_map['Autoscale'] = 'Autoscale is not enabled.'
    return app_summary_map

# %%
def main():
  unravel_url = ''
  unravel_token = ''
  organization_url = ''
  pat = ''
  project_name = ''
  build_id = ''
  
  try:
    opts, args = getopt.getopt(sys.argv[1:], 'hu:t:i:',
      ['unravel=', 'token=', 'prid='])
  except getopt.GetoptError:
    print(
      'githubactionclient.py -u <unravel_url> -t <unravel_api_token> -i <pr_id> ')
    sys.exit(2)

  for opt, arg in opts:
    if opt == '-h':
      print(
        'githubactionclient.py -u <unravel_url> -t <unravel_api_token> -i <pr_id> ')
      sys.exit()
    elif opt in ('-u', '--unravel'):
        unravel_url = arg
    elif opt in ('-t', '--token'):
        unravel_token = arg
    elif opt in ('-i', '--id'):
        pr_id = arg

   

  print('-u : ' + unravel_url)
  print('-t : ' + unravel_token)
  print('-i : ' + pr_id)


  with open('pr_body.txt', 'r') as file:
    raw_description = file.read()

  description = ' '.join(raw_description.splitlines())
  description = re.sub(cleanRe, '', description)
  job_run_list = get_job_runs_from_description_as_text(pr_id, description)

  # start and end TS
  today = datetime.today()
  endDT = datetime(year=today.year, month=today.month, day=today.day, hour=today.hour, second=today.second) 
  startDT = endDT - timedelta(days=14)
  start_time = startDT.astimezone().isoformat()
  end_time = endDT.astimezone().isoformat()
  print('start: ' + start_time)
  print('end: '  + end_time)

  job_run_result_list = []
  for run in job_run_list:
    gsp = run['workspace_id'] + '_' + run['job_id'] + '_' + run['run_id']
    job_runs_json = search_summary_by_globalsearchpattern(unravel_url, unravel_token, start_time, end_time, gsp)
    
    if job_runs_json:
      '''
      gsp_file = gsp + '_summary.json'
      with open(gsp_file, "w") as outfile:
        json.dump(job_runs_json, outfile)
      '''
      clusterUId = job_runs_json[0]['clusterUid']
      appId      = job_runs_json[0]['sparkAppId']
      print("clusterUid: " + clusterUId)
      print("sparkAppId: " + appId)

      result_json = search_analysis(unravel_url, unravel_token, clusterUId, appId)
      if result_json:
        '''
        gsp_file = gsp + '_analysis.json'
        with open(gsp_file, "w") as outfile:
          json.dump(result_json, outfile)
        '''
        insights_json = result_json['insightsV2']
        recommendation_json = result_json['recommendation']
        insights2_json = []
        for item in insights_json:
           #if item['key'] != 'SparkAppTimeReport':
           insights2_json.append(item)
       
        run['unravel_url'] = unravel_url + '/#/jobs/runs'
        run['unravel_keyword'] = gsp
        run['unravel_insights'] = insights2_json
        run['unravel_recommendation'] = recommendation_json
        run["app_summary"] = fetch_app_summary(unravel_url, unravel_token, clusterUId, appId)
        
        # add to the list
        job_run_result_list.append(run)
    else:
       print("job_run not found: " + gsp)


  if job_run_result_list:
    unravel_comments = create_comments_with_markdown(job_run_result_list)
    print(unravel_comments)
    with open('pr_unravel.txt', 'w') as f:
      f.write(unravel_comments)
    
  else:
    print("Nothing to do without Unravel integration")
    sys.exit(0)
     
    
    
     
                                   





# %%
if __name__ == "__main__":
    main()

# %%


# %%



