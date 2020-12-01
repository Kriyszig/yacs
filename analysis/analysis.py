import re
import json
import statistics
from datetime import datetime
import matplotlib.pyplot as plt

class Analyze:
  def __init__(self, CONFIG_PATH = '../config.json', CSV_FORMAT_PATH = 'analysis_{}.csv'):
    self.csv_path = CSV_FORMAT_PATH
    with open(CONFIG_PATH) as conf:
      self.config = json.load(conf)
    self.workers = self.config['workers']
      
  def analyze(self):
    algorithm = None
    time_sum = 0
    count = 0
    
    master_record = []
    points = []
    plots = []
    big_plot = []
    with open('../log/master.log', 'r') as master_log:
      line = master_log.readline().strip(' \n')

      while line:
        algo_match_obj = re.search("Master started on: 127.0.0.1:5000 using the ([A-Z-]+) scheduler.", line)
        time_match_obj = re.search("FINISHED: All tasks of Job ID: [0-9A-Za-z_-]+ in ([0-9.]+)", line)
        plot_match_obj = re.search("PLOT: (.*)$", line)
        stop = re.search("Starting master", line)
        
        if algo_match_obj:
          algorithm = algo_match_obj.group(1)
        if time_match_obj:
          points.append(float(time_match_obj.group(1)))
          time_sum += float(time_match_obj.group(1))
          count += 1
        if plot_match_obj:
          plots.append(plot_match_obj.group(1).split("\t"))
        
        line = master_log.readline().strip()
        
        if stop and algorithm:
          master_record.append([algorithm, count, round(time_sum / count, 5), statistics.median(points)])
          for j in range(len(plots)):
            plots[j][0] = datetime.strptime(plots[j][0], "%m/%d/%Y,%H:%M:%S")
            for k in range(1, len(plots[j])):
              plots[j][k] = int(plots[j][k])
          big_plot.append(plots)
          time_sum = 0
          count = 0
          points = []
          plots = []

      master_record.append([algorithm, count, round(time_sum / count, 5), statistics.median(points)])
      for j in range(len(plots)):
        plots[j][0] = datetime.strptime(plots[j][0], "%m/%d/%Y,%H:%M:%S")
        for k in range(1, len(plots[j])):
          plots[j][k] = int(plots[j][k])
      big_plot.append(plots)
      big_plot = big_plot[-3:]

    time_sum = 0
    count = 0
    worker_records = [[i[0], i[1], 0, 0, 0] for i in master_record]

    for i in range(len(self.workers)):
      flag = False
      k = 0
      points = []
      with open('../log/worker_{}.log'.format(self.workers[i]['port']), 'r') as worker_log:
        line = worker_log.readline().strip(' \n')
        while line:
          match_obj = re.search("ENDING: Task with Task ID: [A-Za-z0-9_-]+ on Worker that ran for ([0-9.]+)s", line)
          stop = re.search('Starting worker', line)
          
          if match_obj:
            points.append(float(match_obj.group(1)))
            time_sum += float(match_obj.group(1))
            count += 1
          line = worker_log.readline().strip()
          
          if stop and flag:
            worker_records[k][2] += time_sum
            worker_records[k][3] += count
            worker_records[k][4] = statistics.median(points)
            time_sum = 0
            count = 0
            points = []
            k += 1
          elif stop:
            flag = True

        worker_records[k][2] += time_sum
        worker_records[k][3] += count
        worker_records[k][4] = statistics.median(points)
        
    # print(master_plots)
    # print(per_worker_stats[0])
            
    with open(self.csv_path.format('master'), 'w') as master_analysis:
      master_analysis.write("Algorithm,Request Count,Mean,Median\n")
      for i in master_record:
        master_analysis.write('{},{},{},{}\n'.format(i[0], i[1], i[2], i[3]))
        
    with open(self.csv_path.format('worker'), 'w') as worker_analysis:
      worker_analysis.write("Algorithm,Request Count,Mean,Total Jobs,Median\n")
      for i in worker_records:
        worker_analysis.write('{},{},{},{},{}\n'.format(i[0], i[1], round(i[2] / i[3], 5), i[3], i[4]))

    algorithms = ['Round Robin', 'Least Loaded', 'Random']
    plt.figure()
    for i in range(3):
      plt.subplot(311 + i)
      dt = [k[0] for k in big_plot[i]]
      for j in range(1, len(big_plot[i][0])):
        plt.scatter(dt, [k[j] for k in big_plot[i]])
      plt.xlabel('Jobs')
      plt.ylabel('Time')
      plt.title(algorithms[i] + " Scheduling")
    plt.tight_layout(pad=0.5)
    #plt.show()
    plt.savefig('plot.jpg')
      
    

if __name__ == '__main__':
  analyzer = Analyze()
  analyzer.analyze()