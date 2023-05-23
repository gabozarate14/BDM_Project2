import configparser
from datetime import datetime
from hdfs import InsecureClient

CONFIG_ROUTE = 'utils/config.cfg'

config = configparser.ConfigParser()
config.read(CONFIG_ROUTE)

host = config.get('data_server', 'host')
user = config.get('data_server', 'user')

hdfs_path = config.get('routes', 'hdfs')
host_hdfs = 'http://' + host + ':9870'
# Connect to hdfs
client = InsecureClient(host_hdfs, user=user)
files = client.list('/user/bdm/model/kpi1')

# client.delete('/user/bdm/model/kp1')



print(files)