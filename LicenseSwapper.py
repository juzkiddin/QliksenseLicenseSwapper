import requests
import json
import urllib3
import csv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def user_id_fetch(user_name_variable,xrfkey,headers,env):
    url = f"https://qliksense-{env}.yourdomain.com/qrs/User/table?filter=(name+so+'{user_name_variable}')&xrfkey={xrfkey}"
    body_object = {
        "entity": "User",
        "columns": [
            {
                "name": "id",
                "columnType": "Property",
                "definition": "id"
            }
        ]
    }
    body_json = json.dumps(body_object, separators=(',', ':'))
    try:
        response = requests.post(url, headers=headers, data=body_json, verify=False, timeout=120)
        if 200 <= response.status_code < 300:
            data = response.json()
            if "rows" in data and len(data["rows"]) > 0:
                user_id = data["rows"][0][0]
                return(user_id)
            else:
                return 0
        else:
            error_text = f"{response.status_code}: {response.text}"
            print(f"Request failed with status code {error_text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
def selection_id_alloc(user_id,xrfkey,headers,env):
    url = f"https://qliksense-{env}.yourdomain.com/qrs/Selection?xrfkey={xrfkey}"
    body_object = {
        "items": [
            {
                "type": "User",
                "objectID": f"{user_id}"
            }
        ]
    }
    body_json = json.dumps(body_object, separators=(',', ':'))
    try:
        response = requests.post(url, headers=headers, data=body_json, verify=False, timeout=120)
        if 200 <= response.status_code < 300:
            data = response.json()
            main_id = data["id"]
            return main_id
        else:
            error_text = f"{response.status_code}: {response.text}"
            print(f"Request failed with status code {error_text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
def load_config(file_path="config.json"):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{file_path}' not found. Please ensure the file exists.")
        raise
    except PermissionError:
        print(f"Error: Permission denied when trying to read '{file_path}'. Please check file permissions.")
        raise
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in '{file_path}'. Please check the file format.")
        print(f"JSON Error details: {e}")
        raise
    except IOError as e:
        print(f"Error: I/O error occurred while reading '{file_path}': {e}")
        raise
    except Exception as e:
        print(f"Error: Unexpected error occurred while loading config file '{file_path}': {e}")
        raise
def license_alloc(selec_id,xrfkey,headers,env,licensetype):
    url = f"https://qliksense-{env}.yourdomain.com/qrs/Selection/{selec_id}/User/License/{licensetype}AccessType?xrfkey={xrfkey}"
    body_object = {"quarantined":"false"}
    body_json = json.dumps(body_object, separators=(',', ':'))
    try:
        response = requests.post(url, headers=headers, data=body_json, verify=False, timeout=120)
        if 200 <= response.status_code < 300:
            data = response.json()
            return data
        else:
            error_text = f"{response.status_code}: {response.text}"
            print(f"Request failed with status code {error_text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
def selection_id_del (xrfkey,selection_id,headers,env):
    url = f"https://qliksense-{env}.yourdomain.com/qrs/Selection/{selection_id}?xrfkey={xrfkey}"
    try:
        response = requests.delete(url, headers=headers, verify=False, timeout=120)
        if 200 <= response.status_code < 300:
            return 1
        else:
            error_text = f"{response.status_code}: {response.text}"
            print(f"Request failed with status code {error_text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
def read_usernames(csv_file):
    usernames = []
    try:
        with open(csv_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Try multiple possible keys
                username = row.get("Name")
                if username:
                    usernames.append(username.strip())
        if not usernames:
            print(f"Warning: No usernames found in '{csv_file}'. Please check if the 'Name' column exists and contains data.")
        return usernames
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found. Please ensure the file exists.")
        raise
    except PermissionError:
        print(f"Error: Permission denied when trying to read '{csv_file}'. Please check file permissions.")
        raise
    except csv.Error as e:
        print(f"Error: CSV parsing error in '{csv_file}'. Please check the file format.")
        print(f"CSV Error details: {e}")
        raise
    except IOError as e:
        print(f"Error: I/O error occurred while reading '{csv_file}': {e}")
        raise
    except Exception as e:
        print(f"Error: Unexpected error occurred while reading CSV file '{csv_file}': {e}")
        raise
def selection_id_dealloc(user_id,xrfkey,headers,env,licensetype):
    url = f"https://qliksense-{env}.yourdomain.com/qrs/Selection?xrfkey={xrfkey}"
    body_object = {
        "items": [
            {
                "type": f"License.{licensetype}AccessType",
                "objectID": f"{user_id}"
            }
        ]
    }
    body_json = json.dumps(body_object, separators=(',', ':'))
    try:
        response = requests.post(url, headers=headers, data=body_json, verify=False, timeout=120)
        if 200 <= response.status_code < 300:
            data = response.json()
            main_id = data["id"]
            return main_id
        else:
            error_text = f"{response.status_code}: {response.text}"
            print(f"Request failed with status code {error_text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
def allocation_id(headers,user_name_variable,xrfkey,env,licensetype) :
    url = f"https://qliksense-{env}.yourdomain.com/qrs/License/{licensetype}AccessType/table?filter=(user.name+so+'{user_name_variable}')&xrfkey={xrfkey}"
    body_object = {
        "entity": f"License.{licensetype}AccessType",
        "columns": [
            {"name": "id", "columnType": "Property", "definition": "id"}
        ]
    }
    body_json = json.dumps(body_object, separators=(',', ':'))
    try:
        response = requests.post(url, headers=headers, data=body_json, verify=False, timeout=120)
        if 200 <= response.status_code < 300:
            data = response.json()
            if "rows" in data and len(data["rows"]) > 0:
                user_id = data["rows"][0][0]
                return(user_id)
            else:
                return 0
        else:
            error_text = f"{response.status_code}: {response.text}"
            print(f"Request failed with status code {error_text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
def license_dealloc(xrfkey,selection_id,headers,env,licensetype):
    url = f"https://qliksense-{env}.yourdomain.com/qrs/Selection/{selection_id}/License/{licensetype}AccessType?xrfkey={xrfkey}"
    try:
        response = requests.delete(url, headers=headers, verify=False, timeout=120)
        if 200 <= response.status_code < 300:
            return 1
        else:
            error_text = f"{response.status_code}: {response.text}"
            print(f"Request failed with status code {error_text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
def license_deallocator(user_name_variable,licensetype,headers,xrfkey,env):
    alloc_id = allocation_id(headers,user_name_variable,xrfkey,env,licensetype)
    if alloc_id == 0:
        print("User is not allocated a license or sending request to server failed. Please check the configuration or user status in QMC")
        print(f"License Deallocation failed for user : {user_name_variable}")
        return 0
    else:
        print("Allocation ID received")
        selec_id = selection_id_dealloc(alloc_id,xrfkey,headers,env,licensetype)
        if selec_id == 0:
            print("Unable to send selection request to server. Please check the configuration")
            print(f"License Deallocation failed for user : {user_name_variable}")
            return 0
        else:
            print("Selection ID received")
            dealloc_status=license_dealloc(xrfkey,selec_id,headers,env,licensetype)
            if dealloc_status == 0:
                print(f"License Deallocation request failed for user : {user_name_variable}")
                selec_id_status=selection_id_del(xrfkey,selec_id,headers,env)
                if selec_id_status == 0:
                    print(f"Cannot delete selection id: {selec_id}")
                    return 0
                print("Deallocation unsuccessful. Selection ID Deleted")
                return 0
            else:
                print("Deallocation Successful")
                selec_id_status=selection_id_del(xrfkey,selec_id,headers,env)
                if selec_id_status == 0:
                    print(f"Cannot delete selection id: {selec_id}")
                    return 1
                print("Selection ID Deleted")
                return 1
def license_allocator(user_name_variable,xrfkey,headers,env,licensetype):
    user_id = user_id_fetch(user_name_variable,xrfkey,headers,env)
    if user_id == 0:
        print("User is not found or sending request to server failed. Please check the configuration or user status in QMC")
        print(f"License Allocation failed for user : {user_name_variable}")
        return 0
    else:
        print("User ID received")
        selec_id = selection_id_alloc(user_id,xrfkey,headers,env)
        if selec_id == 0:
            print("Unable to send selection request to server. Please check the configuration")
            print(f"License Allocation failed for user : {user_name_variable}")
            return 0
        else:
            print("Selection ID received")
            alloc_status=license_alloc(selec_id,xrfkey,headers,env,licensetype)
            if alloc_status == 0:
                print(f"License Allocation request failed for user : {user_name_variable}")
                selec_id_status=selection_id_del(xrfkey,selec_id,headers,env)
                if selec_id_status == 0:
                    print(f"Cannot delete selection id: {selec_id}")
                    return 0
                print("Allocation unsuccessful. Selection ID Deleted")
                return 0
            else:
                print("Allocation Successful")
                selec_id_status=selection_id_del(xrfkey,selec_id,headers,env)
                if selec_id_status == 0:
                    print(f"Cannot delete selection id: {selec_id}")
                    return 1
                print("Selection ID Deleted")
                return 1
def main(user_name_variable,swaptype):
    try:
        config = load_config()
    except (FileNotFoundError, PermissionError, json.JSONDecodeError, IOError, Exception):
        print("Cannot proceed without valid configuration. Exiting.")
        return
    
    cookie = config.get("cookie")
    env = config.get("env")
    xrfkey = config.get("xrfkey")
    
    # Validate required config values
    if not cookie:
        print("Error: 'cookie' not found in configuration file. Please check config.json")
        return
    if not env:
        print("Error: 'env' not found in configuration file. Please check config.json")
        return
    if not xrfkey:
        print("Error: 'xrfkey' not found in configuration file. Please check config.json")
        return
    headers = {
        "Accept" : "application/json, text/plain, */*",
        "Accept-Encoding" : "gzip, deflate, br, zstd",
        "Accept-Language" : "en-US",
        "Cookie": cookie,
        "Host" : f"qliksense-{env}.yourdomain.com",
        "Origin" : f"https://qliksense-{env}.yourdomain.com",
        "Referer" : f"https://qliksense-{env}.yourdomain.com/qmc/professionalaccessallocations",
        "Sec-Fetch-Dest" : "empty",
        "Sec-Fetch-Mode" : "cors",
        "Sec-Fetch-Site" : "same-origin",
        "sec-ch-ua" : "\"Chromium\";v=\"142\", \"Google Chrome\";v=\"142\", \"Not_A Brand\";v=\"99\"",
        "sec-ch-ua-mobile" : "?0",
        "sec-ch-ua-platform" : "\"Windows\"",
        "Connection" : "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "X-Qlik-xrfkey": xrfkey,
        "Content-Type": "application/json;charset=UTF-8"
    }
    if swaptype == 1:
        print(f"Switching License Type from Analyzer to Professional for user {user_name_variable}")
        print("Started Professional License Allocation")
        license_alloc_status = license_allocator(user_name_variable,xrfkey,headers,env,"Professional")
        if license_alloc_status == 0:
            print("Allocation Process exited as license could not be allocated")
        else:
            print("Started Analyzer License Deallocation")
            license_dealloc_status = license_deallocator(user_name_variable,"Analyzer",headers,xrfkey,env)
            if license_dealloc_status == 0:
                print("Deallocation Process exited as license could not be deallocated. Please deallocate manually for this user")
            else:
                print(f"Swapping License from Analyzer to Professional for user {user_name_variable} is completed Successfully")
    else:
        if swaptype == 2:
            print(f"Switching License Type from Professional to Analyzer for user {user_name_variable}")
            print("Started Analyzer License Allocation")
            license_alloc_status = license_allocator(user_name_variable,xrfkey,headers,env,"Analyzer")
            if license_alloc_status == 0:
                print("Allocation Process exited as license could not be allocated")
            else:
                print("Started Professional License Deallocation")
                license_dealloc_status = license_deallocator(user_name_variable,"Professional",headers,xrfkey,env)
                if license_dealloc_status == 0:
                    print("Deallocation Process exited as license could not be deallocated. Please deallocate manually for this user")
                else:
                    print(f"Swapping License from Professional to Analyzer for user {user_name_variable} is completed Successfully")
        else:
            print("Swaptype selection invalid")    
if __name__ == "__main__":
    try:
        usernames = read_usernames("QS_ST_Approval2_CORP_SharedServices.csv")
        if not usernames:
            print("No usernames to process. Exiting.")
            exit(1)
        print(f"Found {len(usernames)} username(s) to process: {usernames}")
        swaptype = 1 #1 for A->P and 2 for P->A
        for user_name_variable in usernames:
            main(user_name_variable,swaptype)
            print(f"Executed for {user_name_variable}")
    except (FileNotFoundError, PermissionError, csv.Error, IOError, Exception):
        print("Cannot proceed without valid CSV file. Exiting.")
        exit(1)            
