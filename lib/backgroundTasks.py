import datetime
from gi.repository import GObject
# Background Tasks #
##########################################################################
# * watch for tasks and show them in the TaskManager
# * Monitor host states and change them in de hostlist
class BackgroundTasks(object):
  def __init__(self,interval=2):
    self.stop_thread = False
    self.last_run = datetime.datetime.now()
    self.jobs = {}
    self.interval = interval
  def addJob(self,job):
    if job.__name__ in self.jobs:
      raise NameError('Job "' + self.jobs[job].__name__ + '" already registered')
    else:
      self.jobs[job.__name__] = job
  def start(self):
    GObject.timeout_add(self.interval*1000,self.run)
  def run(self):
    for job in self.jobs:
      print "running: job",self.jobs[job].__name__

      j = self.jobs[job]
      try:
        j.run()
      except:
        raise NotImplementedError('Job "' + self.jobs[job].__name__ + '" has no "run" method')
    if self.stop_thread:
      return False
    else:
      return True
  def getJob(self,jobName):
    if jobName in self.jobs and not (self.jobs[jobName] is None):
      return self.jobs[jobName]
    else:
      raise KeyError('no job known as "' + jobName + '"')

class Job(object):
  __name__ = "JobName"
  def run(self):
    print "your Task to run"

class VIMTasks(Job):
  __name__ = "VIMTasks"
  def __init__(self,taskList):
    self.tasks = []
    self._tasks = []
    self.taskList = taskList
    self.last_run = datetime.datetime.now()
    self.runNow = False
  def run(self):
    print "running tasks"
    if (datetime.datetime.now()-self.last_run).total_seconds >= 2 or self.runNow:
      self._tasks = []

      if self.tasks:
        self.taskList.clear()
        taskid = 0
        for task in self.tasks:
          taskid = taskid + 1
          info = task["task"].get_state()
          progress = task["task"].get_progress()
          print task["name"],progress
          if type(progress) is not int:
            progress = 100
          if info != VITask.STATE_SUCCESS or (datetime.datetime.now()-task["start"]).total_seconds() < 60:
            self._tasks.append(task)
            self.taskList.append((taskid,task["name"] + "(" + info + ")",task["start"].strftime('%c'),task["task"].get_progress()))
      self.task = self._tasks[:]
      self.last_run = datetime.datetime.now()
      self.runNow = False
    return True
  def add(self,name,task):
    self.tasks.append({"name":name,"start":datetime.datetime.now(),"task":task})
    self.runNow = True
  def listTasks(self):
    return self.tasks