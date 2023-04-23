from datetime import datetime

git_conf_location = "/cf-persistent-config/confdb"
git_backup_location = "/cf-persistent-config"


@property
def backup_file_name():
    now = datetime.now()
    return git_backup_location + "/backup-" + now.strftime("%Y-%m-%d_%H-%M-%S")
