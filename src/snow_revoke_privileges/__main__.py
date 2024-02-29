"""__main__.py"""

from snow_revoke_privileges.application import Application

if __name__ == "__main__":
    app: Application = Application()
    app.execute()
