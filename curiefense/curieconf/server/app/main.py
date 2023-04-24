from curieconf.confserver import app
from curieconf.confserver.backend import Backends

import os

git_conf_location = "/cf-persistent-config/confdb"
app.backend = Backends.get_backend(app, f"git://${git_conf_location}")
options = {}
val = os.environ.get("CURIECONF_TRUSTED_USERNAME_HEADER", None)
if val:
    options["trusted_username_header"] = val
val = os.environ.get("CURIECONF_TRUSTED_EMAIL_HEADER", None)
if val:
    options["trusted_email_header"] = val

app.options = options
