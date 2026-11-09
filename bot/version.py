"""The app's version, one place for all of it: the server reports it (GET /api/public/info, which the web app's header
shows as "V1.3.0") and the launcher's window title reads it from this file. The connector keeps its own VERSION
(connector/connector.py): the server gates plans on that one."""
APP_VERSION = "1.3.2"       # every release bumps it
