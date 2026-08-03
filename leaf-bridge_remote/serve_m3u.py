#!/usr/bin/env python3
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
os.chdir('/root/leaf-bridge')
HTTPServer(('127.0.0.1', 9797), SimpleHTTPRequestHandler).serve_forever()
