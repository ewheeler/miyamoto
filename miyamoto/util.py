import random

import gevent.socket

def line_protocol(socket, strip=True):
    fileobj = socket.makefile()
    while True:
        try:
            line = fileobj.readline() # returns None on EOF
            if line is not None and strip:
                line = line.strip()
        except IOError:
            line = None
        if line:
            yield line
        else:
            break

def connect_and_retry(address, source_address=None, max_retries=None):
    max_delay = 3600
    factor = 2.7182818284590451 # (math.e)
    jitter = 0.11962656472 # molar Planck constant times c, joule meter/mole
    delay = 1.0
    retries = 0
    
    while True:
        try:
            return gevent.socket.create_connection(address, source_address=source_address)
        except IOError:
            retries += 1
            if max_retries is not None and (retries > max_retries):
                raise IOError("Unable to connect after %s retries" % max_retries)
            delay = min(delay * factor, max_delay)
            delay = random.normalvariate(delay, delay * jitter)
            gevent.sleep(delay)

JSON_CONTENT_TYPES = ['application/json', 'application/json; charset=utf-8', 'application/x-javascript', 'text/javascript', 'text/x-javascript', 'text/x-json']

def walks_like_json(raw_content_type):
    if ';' in raw_content_type:
        content_type = raw_content_type.split(';')[0]
    else:
        content_type = raw_content_type
    if content_type in JSON_CONTENT_TYPES:
        return True
    if 'json' in content_type:
        return True
    if 'javascript' in content_type:
        return True
    if 'ecma' in content_type:
        return True
    return False

FORM_CONTENT_TYPES = ['application/x-www-form-urlencoded', 'application/x-www-form-urlencoded; charset=utf-8']

def talks_like_form(raw_content_type):
    return raw_content_type in FORM_CONTENT_TYPES
