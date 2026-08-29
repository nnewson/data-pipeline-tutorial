import os

# Defaults target the host. Processes running inside the Compose network
# override these, because the same service answers to a different address
# there.
WEB_URL = os.environ.get("WEB_URL", "http://localhost:8080")
