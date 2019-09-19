How to execute server-side code to populate databases:
  
    1.) python run_dir.py get_data

    2.) python run_dir.py merge_data

    3.) python run_dir.py optimize_data
  
    These commands must be executed in this exact order since the execution algorithm ensures that collections are not read from before they are created



How to Run Web App:

    1.) Run Flask app (python app.py) make sure to run on 8080

    2.) Open a chrome browser with security disabled (CORS issue):

    For PC: chrome.exe --user-data-dir="C://Chrome dev session" --disable-web-security

    For Mac: open -n -a /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --args --user-data-dir="/tmp/chrome_dev_test" --disable-web-security

    3.) Open index.html inside that chrome browser (whole AngularJS front end is in this file; all js libraries are accessed with CDN's)  

    4.) If that doesn't work, either try quitting chrome, or using Firefox with a CORS plugin

  
Please read project writeup for indepth explinations on the purpose and functionality of this project

  
