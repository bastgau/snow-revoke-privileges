"""__main__.py"""

from typing import List
from snowflake_reset.reset_privilege import ResetPrivilege

working_databases: List[str] = []
default_owner: str = "SYSADMIN"

if __name__ == "__main__":
    reset_privilege: ResetPrivilege = ResetPrivilege()
    reset_privilege.execute(working_databases, default_owner, "ACCOUNTADMIN")
