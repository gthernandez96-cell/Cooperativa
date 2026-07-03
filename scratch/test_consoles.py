import urllib.request
import json
import ssl

PA_USER = "gthernandez96"
PA_API_TOKEN = "1e628583fd7c3b8aeadcf91b5ae29aa820f6daea"
BASE_URL = f"https://www.pythonanywhere.com/api/v0/user/{PA_USER}"
HEADERS = {"Authorization": f"Token {PA_API_TOKEN}"}

req = urllib.request.Request(f"{BASE_URL}/consoles/", headers=HEADERS)
context = ssl._create_unverified_context()
try:
    with urllib.request.urlopen(req, context=context) as r:
        print("CONSOLES LIST:")
        print(r.read().decode())
except Exception as e:
    print("Error:", e)
    if hasattr(e, 'read'):
        print(e.read().decode())
