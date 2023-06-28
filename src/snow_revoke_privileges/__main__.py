"""__main__.py"""

from snow_revoke_privileges.reset_privilege import ResetPrivilege

if __name__ == "__main__":
    reset_privilege: ResetPrivilege = ResetPrivilege()
    reset_privilege.execute()
