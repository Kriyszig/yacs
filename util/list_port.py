import json

if __name__ == '__main__':
  with open('config.json') as conf:
    config = json.load(conf)
  workers = config['workers']
  port_str = '{}'.format(workers[0]['port'])
  for i in range(1, len(workers)):
    port_str += ';{}'.format(workers[i]['port'])
    
  print(port_str)