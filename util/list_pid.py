if __name__ == '__main__':
  pid_str = ''
  with open('util/pid.txt', 'r') as pid_file:
    line = pid_file.readline().strip(' \n')
    pid_str += line
    line = pid_file.readline().strip(' \n')
    while line:
      pid_str += ";{}".format(line)
      line = pid_file.readline().strip(' \n')
      
  print(pid_str)