import urllib.request
import json
import ssl

PA_USER = "gthernandez96"
PA_API_TOKEN = "1e628583fd7c3b8aeadcf91b5ae29aa820f6daea"
BASE_URL = f"https://www.pythonanywhere.com/api/v0/user/{PA_USER}"
HEADERS = {"Authorization": f"Token {PA_API_TOKEN}"}

# Request the WSGI file content
wsgi_path = f"/var/www/{PA_USER}_pythonanywhere_com_wsgi.py"
url = f"{BASE_URL}/files/path{wsgi_path}"
req = urllib.request.Request(url, headers=HEADERS)
context = ssl._create_unverified_context()

try:
    with urllib.request.urlopen(req, context=context) as r:
        print("WSGI FILE CONTENT:")
        print(r.read().decode('utf-8'))
except Exception as e:
    print("Error:", e)
    if hasattr(e, 'read'):
        print(e.read().decode('utf-8'))
