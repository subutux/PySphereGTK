#!/usr/bin/env python
import gi
import pygtk
import time
import datetime
import pprint
import re
import sys
from pysphere import *
from gi.repository import Gtk as gtk
from gi.repository import Gdk as gdk
from gi.repository import GdkPixbuf
from gi.repository import GObject
from lib.images import pysphereImages
from lib.vars import *
# from lib.backgroundTasks import *
import threading
gdk.threads_init()
# Background Tasks #
##########################################################################
# * watch for tasks and show them in the TaskManager
# * Monitor host states and change them in de hostlist
class BackgroundTasks(object):
  def __init__(self,interval=3):
    self.stop_thread = False
    self.last_run = datetime.datetime.now()
    self.jobs = {}
    self.interval = interval
  def addJob(self,job):
    if job.__name__ in self.jobs:
      raise NameError('Job "' + self.jobs[job].__name__ + '" already registered')
    else:
      self.jobs[job.__name__] = job
      print "job",self.jobs[job.__name__].__name__,"added"

  def start(self):
    GObject.timeout_add(self.interval*1000,self.run)
  def stop(self):
    self.stop_thread = True
  def run(self):
    for job in self.jobs:
      # print "running: job",self.jobs[job].__name__
      if self.stop_thread:
        return False
      j = self.jobs[job]
      try:
        # start = time.clock()
        j.run();
        # end = time.clock()
        # print 'job duration: %.6f seconds' % (end - start)
      except Exception,e:
        #raise NotImplementedError('Job "' + self.jobs[job].__name__ + '" has no "run" method')
        print "Error running", j.__name__,":"
        print Exception
        print e
        pass

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
    # print "running tasks"

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
          if info == "running":
            icon = pysphereImages.WAIT_ICON_16
          elif info == "success":
            icon = pysphereImages.OK_ICON_16
          elif info == "error":
            icon = pysphereImages.CRIT_ICON_16
          else:
            icon = pysphereImages.WARN_ICON_16


          if info != VITask.STATE_SUCCESS or (datetime.datetime.now()-task["start"]).total_seconds() < 60:
            self._tasks.append(task)

            self.taskList.append((taskid,task["name"] + "(" + info + ")",task["start"].strftime('%c'),task["task"].get_progress(),icon))
        self.tasks = []
        self.tasks = self._tasks[:]
      self.last_run = datetime.datetime.now()
      self.runNow = False
      return True
  def add(self,name,task):
    self.tasks.append({"name":name,"start":datetime.datetime.now(),"task":task})
    self.runNow = True
  def listTasks(self):
    return self.tasks

class VMStatusJob(Job):
  __name__ = "VMStatusJob"
  def __init__(self,server,treestore_vms):
    self.treestore = treestore_vms
    self.server = server
    self.previous_states = {
    "pOn" : None,
    "pOff" : None,
    "susp" : None,
    "err" : None
    }
  def change_tree(self,store,treeiter,colSearch,val):
    while treeiter != None:
      # if type(colVal) != "list":
      #   raise TypeError("colVal is not a list but a " + type(colSearch))

      # if type(val) != "dict":
      #   raise TypeError("val is not a dict but a " + type(colSearch))

      if store.iter_has_child(treeiter):
        childiter = store.iter_children(treeiter)
        self.change_tree(store,childiter,colSearch,val)
      if store[treeiter][colSearch]:
        if store[treeiter][colSearch] in val:
          if store[treeiter][val[store[treeiter][colSearch]][0]]:
            store[treeiter][val[store[treeiter][colSearch]][0]] = val[store[treeiter][colSearch]][1]
            
      treeiter = store.iter_next(treeiter)
  def run(self):
    pOn = self.server.get_registered_vms(status='poweredOn')
    pOff = self.server.get_registered_vms(status='poweredOff')
    susp = self.server.get_registered_vms(status='suspended')
    err = self.server.get_registered_vms(advanced_filters={
      'summary.overallStatus':['red','yellow']
      })
    first = self.treestore.get_iter_first()
    if pOn != self.previous_states["pOn"]:
      self.previous_states["pOn"] = pOn[:]
      if pOn:
        change_pOn = {}
        for vm in pOn:
          change_pOn[vm] = [4,pysphereImages.VM_PLAY_ICON_16]
        self.change_tree(self.treestore,first,2,change_pOn)
    if pOff != self.previous_states["pOff"]:
      self.previous_states["pOff"] = pOff[:]
      if pOff:
        change_pOff = {}
        for vm in pOff:
          change_pOff[vm] = [4,pysphereImages.VM_ICON_16]
        self.change_tree(self.treestore,first,2,change_pOff)
    if susp != self.previous_states["susp"]:
      self.previous_states["susp"] = susp[:]
      if susp:
        change_susp = {}
        for vm in susp:
          change_susp[vm] = [4,pysphereImages.VM_PAUZE_ICON_16]
        self.change_tree(self.treestore,first,2,change_susp)
    if err != self.previous_states["err"]:
      self.previous_states["err"] = err[:]
      if err:
        change_err = {}
        for vm in err:
          change_err[vm] = [4,pysphereImages.VM_WARN_ICON_16]
        self.change_tree(self.treestore,first,2,change_err)

    return True

class GUI(object):
  def __init__(self):
      self.builder = gtk.Builder()
      self.builder.add_from_file("ui.ui")
      self.builder.connect_signals(self)
      self.window1 = self.builder.get_object("window_main")
      # self.window1.show()
      self.window_login = self.builder.get_object('window_login')
      self.window_logout = self.builder.get_object('window_disconnect')
      self.treeview = self.builder.get_object('tree_vms')
      self.vsphere = VIServer()
      self.window_login.show()
      # self.window_snapshots = self.builder.get_object('window_snapshot')
      self.spinner = self.builder.get_object('spinner1')
      self.BackgroundTasks = BackgroundTasks()
      self.BackgroundTasks.addJob(VIMTasks(self.builder.get_object('list_tasks')))
      self.tasks = self.BackgroundTasks.getJob("VIMTasks")
      self.vms = None
      self.statusbar = self.builder.get_object('statusbar')
      self.statusbar_context = self.statusbar.get_context_id("running_status")
      self.VMQuestion = None

  def disconnect(self):
    try:
      self.BackgroundTasks.stop()
      self.vsphere.disconnect()
    except:
      print "logout exception"
      return False
    return True

  def on_window_main_destroy(self,widget,data=None):
  	self.window_logout.show()
  	disconnect = threading.Thread(target=self.disconnect())
  	while disconnect.isAlive():
  		while gtk.events_pending():
  			gtk.main_iteration()
  	gtk.main_quit()
  def on_button_connect_cancel(self,widget,data=None):
  	  self.on_window_main_destroy(None)
  def on_button_connect_signin(self,widget,data=None):

    #server = self.builder.get_object('entry_connect_server').get_text()
    server = self.builder.get_object('entry_vsphere_server').get_text()
    self.server = server
    user = self.builder.get_object('entry_vsphere_username').get_text()
    #user = self.builder.get_object('entry_connect_user').get_text()
    password = self.builder.get_object('entry_vsphere_password').get_text()
    #password = self.builder.get_object('entry_connect_password').get_text()
    #elf.builder.get_object('label8').set_text('Logging in ...')
    self.builder.get_object('label_status').set_text('Logging in ...')
    self.builder.get_object('button_vsphere_login').set_sensitive(False)
    doLogin = threading.Thread(target=self.vsphere.connect,args=(server,user,password))
    self.spinner.set_visible(True)
    doLogin.start()
    while doLogin.isAlive():
    	while gtk.events_pending():
    		gtk.main_iteration()

    self.spinner.set_visible(False)
    
    if not self.vsphere.is_connected():
      self.builder.get_object('label_status').set_text('Could not login.')
      self.builder.get_object('button_vsphere_login').set_sensitive(True)
    else:
      self.vms = self.vsphere.get_registered_vms()
      self.BackgroundTasks.start()
      self.spinner.set_visible(True)
      self.builder.get_object('label_status').set_text('Fetching VM Details ...')
      fetch = threading.Thread(target=self.init_fetch,args=())
      fetch.start()
      while fetch.isAlive():
      	while gtk.events_pending():
      		gtk.main_iteration()
      self.spinner.set_visible(False)

      self.window_login.hide()
      self.window1.show()
  def init_fetch(self):
    self.vms_full={}
    vms_table = {}
    self.pm = self.vsphere.get_performance_manager()
    table = self.builder.get_object('treestore_vms')
    vm_id = 0
    parent = table.append(None,(0,str(self.server),str('[SERVER] ' + self.server),None,pysphereImages.SERVER_ICON_32,liststore_vars.ESXServer))
    for vmpath in self.vms:
      vm_id = vm_id + 1
      self.builder.get_object('label_status').set_text('Loading inventory: ' +  str(vm_id) + '/' + str(len(self.vms) + 1))
      self.vms_full[vmpath] = {"serverObj": self.vsphere.get_vm_by_path(vmpath)}
      name = self.vms_full[vmpath]["serverObj"].get_property('name')
      status = self.vms_full[vmpath]["serverObj"].get_status(basic_status=True)
      if status == "POWERED ON":
        color = "#A9FFAB"
      else:
        color = None
      imgStatus = pysphereImages.VM_ICON_16
      table.append(parent,[vm_id,name,vmpath,color,imgStatus,liststore_vars.VirtualMachine])
    # Add a job to refresh the machines statuses

    self.BackgroundTasks.addJob(VMStatusJob(self.vsphere,self.builder.get_object('treestore_vms')))


  def get_vm_properties(self,vmpath):
    if "properties" not in self.vms_full[vmpath] or (datetime.datetime.now()-self.vms_full[vmpath]["properties"]["last_update"]).total_seconds() > 30:
      self.statusbar.push(self.statusbar_context,'Fetching VM details ...')
      fetch = threading.Thread(target=self._fetch_vm_properties,args=(str(vmpath),None))
      self.statusbar.pop(self.statusbar_context)
      fetch.start()
      while fetch.isAlive():
        while gtk.events_pending():
          gtk.main_iteration()

    return self.vms_full[vmpath]
  def _fetch_vm_properties(self,vmpath,data=None):
    self.vms_full[vmpath]["properties"] = self.vms_full[vmpath]["serverObj"].get_properties()
    datastoreregex = re.compile('\[\w+\]')
    datastore = datastoreregex.search(vmpath).group()
    status = self.vms_full[vmpath]["serverObj"].get_status()
    tools = self.vms_full[vmpath]["serverObj"].get_tools_status()
    cpu_stat = self.pm.get_entity_statistic(self.vms_full[vmpath]["serverObj"]._mor,[5])
    cpus = []
    """
    MOR: 128
    Counter: usagemhz
    Group: CPU
    Description: CPU Usage in MHz (Average)
    Instance: 1
    Value: 150
    Unit: MHz
    Time: 2012-05-02 14:44:23.535254
    """
    for stat in cpu_stat:
      cpus.append({
        "Counter":stat.counter,
        "Group":stat.group,
        "Description": stat.description,
        "Instance": stat.instance,
        "Value": stat.value,
        "Unit": stat.unit,
        "Time": stat.time
        })

    self.vms_full[vmpath]["properties"]["datastore"] = datastore
    self.vms_full[vmpath]["properties"]["state"] = status
    self.vms_full[vmpath]["properties"]["state_tools"] = tools
    self.vms_full[vmpath]["properties"]["cpu_stats"] = cpus
    self.vms_full[vmpath]["properties"]["last_update"] = datetime.datetime.now()


  def statusbarMsg(msg,timeout=5):
    self.statusbar.pop(self.statusbar_context)
    self.statusbar.push(self.statusbar_context,msg)
    if timeout:
      GObject.timeout_add(timeout*1000,self.statusbar.pop(self.statusbar_context))


  def init_fullfetch(self):
    
    vm_id = 0
    table = self.builder.get_object('liststore_vms')
    for i in self.vms:
      print "Fetching:",i
      self.builder.get_object('label8').set_text('Fetching: ' +  str(vm_id) + '/' + str(len(self.vms)))
      self.vms_full[i] = {}
      self.vms_full[i]["serverObj"] = self.vsphere.get_vm_by_path(i)
      self.vms_full[i]["properties"] = self.vms_full[i]["serverObj"].get_properties()
      datastoreregex = re.compile('\[\w+\]')
      datastore = datastoreregex.search(i).group()
      # pprint.pprint(self.vms_full[i]["properties"],indent=4)
      status = self.vms_full[i]["serverObj"].get_status()
      tools = self.vms_full[i]["serverObj"].get_tools_status()
      if status == "POWERED ON":
        color = "#A9FFAB"
      else:
        color = None
      imgStatus = pysphereImages.VM_ICON_32

      self.vms_full[i]["properties"]["datastore"] = datastore
      self.vms_full[i]["properties"]["state"] = status
      self.vms_full[i]["properties"]["state_tools"] = tools

      row = (vm_id,self.vms_full[i]["properties"]["name"],i,color,imgStatus)
      table.append(row)
      vm_id = vm_id + 1
  def on_tree_vm_selection(self,widget,data=None):
    table = self.builder.get_object('treestore_vms')
    tree = self.builder.get_object('tree_vms')
    vm_type,tmp = self.get_treeview_selection(tree,5)
    if vm_type == liststore_vars.VirtualMachine:
      vm_id,tree_id = self.get_treeview_selection(tree,2)
      vm_all = self.get_vm_properties(vm_id)
      vm = vm_all["properties"]
      # pprint.pprint(vm,indent=4)
      self.builder.get_object('label_vm_state').set_text(vm["state"])
      if vm["state"] == VMPowerState.BLOCKED_ON_MSG:
        self._display_question(vm)

      self.builder.get_object('label_vm_name').set_text(vm["name"])
      self.builder.get_object('label_vm_os').set_text(vm["guest_full_name"])
      self.builder.get_object('label_vm_cpus').set_text(str(vm["num_cpu"]) + " vCPU")
      self.builder.get_object('label_vm_mem').set_text(str(vm["memory_mb"]) + " MB")
      self.builder.get_object('label_vm_tools').set_text(vm["state_tools"])
      self.builder.get_object('label_vm_host').set_text(self.server)
      self.fill_datastores('list_datastores',(vm["datastore"]))
      cpu_text = ""
      pprint.pprint(vm["cpu_stats"],indent=4)
      for cpu in vm["cpu_stats"]:
       # pprint.pprint(cpu)
        if cpu["Instance"] == "":
          cpu["Instance"] = "Average"
          cpu_text = cpu_text + "CPU " + cpu['Instance'] + " : " + cpu["Value"] + " " + cpu["Unit"] + "\n"
     # pprint.pprint(cpu_text)
      self.builder.get_object('label_cpu_util').set_text(cpu_text)

    # self.builder.get_object('label_vm_datastore').set_text(vm["datastore"])
  ######################################################################################
  # VMQuestions #
  ###############
  def _display_question(self,vm):
    pprint.pprint(vm)
    vmObj = self.vsphere.get_vm_by_path(vm['path'])
    VMQuestion = vmObj.get_question()
    box = self.builder.get_object('box_question_answers')
    self.builder.get_object('label_question_text').set_text(VMQuestion.text())
    radio = None
    for answer in VMQuestion.choices():
      print answer

      radio = gtk.RadioButton(group=radio,label=answer[1])
      if radio == None:
        radio.set_active(True)
      radio.connect("toggled",self._answer_for_question,answer[0])
      box.pack_start(radio,True,True,0)
      radio.show()
    self.builder.get_object('window_question').show()
    self.VMQuestion = VMQuestion
  def _answer_for_question(self,widget,data=None):
    self.question_answer = data
    print data
  def on_question_answered(self,widget,data=None):
    self.VMQuestion.answer(choice=self.question_answer)
    self.on_question_canceled(None,None)

  def on_question_canceled(self,widget,data=None):
    self.builder.get_object('window_question').hide()
    # empty the vbox again for future use
    for e in self.builder.get_object('box_question_answers').get_children():
      self.builder.get_object('box_question_answers').remove(e)
  ###############
  # VMQuestions #
  ######################################################################################
  def fill_datastores(self,liststore,datastores):
    table = self.builder.get_object(liststore)
    table.clear()
    if type(datastores) is str:
      table.append((0,datastores,0,pysphereImages.DATASTORE_ICON_32))
    else:
      for i in datastores:
        table.append((0,i,0,pysphereImages.DATASTORE_ICON_32))
  def get_treeview_selection(self,tree,id_int=0):
    """
    Get The selection of a treeview and returns a tulip with the contents
    of the clicked col on the row and defined col id_int
    @return (string,string)
    """
    selection = tree.get_selection()
    tree_model,tree_iter = selection.get_selected()
    print tree_model,tree_iter
    return (tree_model.get_value(tree_iter, id_int),tree_model.get_string_from_iter(tree_iter))
  def on_vm_start(self,widget,data=None):
    """
    @gtkcallback
    starts a vm
    """
    tree = self.builder.get_object('tree_vms')
    vm_id,tree_id = self.get_treeview_selection(tree,2)
    vm_all = self.get_vm_properties(vm_id)
    self.tasks.add("Power On " + vm_all["properties"]["name"],vm_all["serverObj"].power_on(sync_run=False))

  def on_vm_suspend(self,widget,data=None):
    """
    @gtkcallback
    Suspends a vm
    """
    tree = self.builder.get_object('tree_vms')
    vm_id,tree_id = self.get_treeview_selection(tree,2)
    vm_all = self.get_vm_properties(vm_id)
    self.tasks.add("Suspend " + vm_all["properties"]["name"],vm_all["serverObj"].suspend(sync_run=False))

  def on_vm_stop(self,widget,data=None):
    """
    @gtkcallback
    Stops a vm
    """
    tree = self.builder.get_object('tree_vms')
    vm_id,tree_id = self.get_treeview_selection(tree,2)
    vm_all = self.get_vm_properties(vm_id)
    self.tasks.add("Power Off " + vm_all["properties"]["name"],vm_all["serverObj"].power_off(sync_run=False))

  def on_vm_reset(self,widget,data=None):
    """
    @gtkcallback
    Resets a vm
    """
    tree = self.builder.get_object('tree_vms')
    vm_id,tree_id = self.get_treeview_selection(tree,2)
    vm_all = self.get_vm_properties(vm_id)
    self.tasks.add("Reset " + vm_all["properties"]["name"],vm_all["serverObj"].reset(sync_run=False))
  ########################################################################################
  # SNAPSHOPTS #
  ##############
  def _get_snapshot_obj(self,snapshot):
    ss = {
      "Name": snapshot.get_name(),
      "Description": snapshot.get_description(),
      "Created": snapshot.get_create_time(),
      "State": snapshot.get_state(),
      "Path": snapshot.get_path(),
      "Children": snapshot.get_children(),
      "Parent": snapshot.get_parent()
      }
    return ss
  def get_snapshots(self,vmpath):

    
    snapshot_list = self.vms_full[vmpath]["serverObj"].get_snapshots()
    snapshot_tree = []
    for snapshot in snapshot_list:
      ss = self._get_snapshot_obj(snapshot)
      
      if ss["Children"]:
        ss["ChildrenFull"] = []
        for child in ss["Children"]:
          ss["ChildrenFull"].append({
            "obj": child,
            "info": self._get_snapshot_obj(child)
            })
      if ss["Parent"]:
        ss["ParentFull"] = self._get_snapshot_obj(ss["Parent"])

      snapshot_tree.append(ss)
    return snapshot_tree
  def _snapshot_datetime_str(self,dt):
    dt_string = str(dt[0]) + "-" + str(dt[1]) + "-" + str(dt[2]) + " " + str(dt[3]) + ":" + str(dt[4])
    return dt_string
  def on_click_vm_show_snapshots(self,widget,data=None):
    self.window_snapshots = self.builder.get_object('window_snapshot')
    table = self.builder.get_object('treestore_snapshots')
    tree = self.builder.get_object('tree_vms')
    vmpath,tmp = self.get_treeview_selection(tree,2)
    snapshots = self.get_snapshots(vmpath)
    added_snapshots = {}

    ss_id = 0
    for snapshot in snapshots:
      if not snapshot['Parent']:
        ss_id = ss_id + 1
        # added_snapshots[snapshot['Path']]
        added_snapshots[snapshot['Path']] = table.append(None,(ss_id,snapshot["Name"] + " (" + snapshot["State"] + ")",self._snapshot_datetime_str(snapshot["Created"]),pysphereImages.SERVER_ICON_32,snapshot["Description"],snapshot["Path"]))
      else:
        if added_snapshots.has_key(snapshot["ParentFull"]["Path"]):
          added_snapshots[snapshot['Path']] = table.append(added_snapshots[snapshot["ParentFull"]["Path"]],(ss_id,snapshot["Name"] + " (" + snapshot["State"] + ")",self._snapshot_datetime_str(snapshot["Created"]),pysphereImages.SERVER_ICON_32,snapshot["Description"],snapshot["Path"]))
                   
      # if "ChildrenFull" in snapshot:
      #     for child_snapshot in snapshot['ChildrenFull']:
      #       table.append(parent,(ss_id,child_snapshot["info"]["Name"] + " (" + child_snapshot["info"]["State"] + ")",self._snapshot_datetime_str(child_snapshot["info"]["Created"]),pysphereImages.SERVER_ICON_32,child_snapshot["info"]["Description"]))
        
    self.window_snapshots.show()
  def on_select_snapshot(self,widget,data=None):
    tree = self.builder.get_object('tree_snapshots')
    name,tmp = self.get_treeview_selection(tree,1)
    descr,tmp = self.get_treeview_selection(tree,4)
    self.builder.get_object('label_snapshot_name').set_text(name)
    self.builder.get_object('label_snapshot_description').set_text(descr)

  def on_destroy_window_snapshots(self,widget,data=None):
    print "Destroy and rule!"
    self.builder.get_object('treestore_snapshots').clear()
    self.window_snapshots.hide()
    # self.window_snapshots = None
    return True
  def on_revert_snapshot(self,widget,data=None):
    tree = self.builder.get_object('tree_snapshots')
    treevm = self.builder.get_object('tree_vms')
    path,tmp = self.get_treeview_selection(tree,5)
    name,tmp = self.get_treeview_selection(tree,1)
    vmpath, tmp = self.get_treeview_selection(treevm,2)
    vm = self.vms_full[vmpath]["serverObj"]

    self.tasks.add('Revert to snapshot ' + name,vm.revert_to_path(path,sync_run=False) )
    self.window_snapshots.hide()
  ##############
  # SNAPSHOPTS #
  ########################################################################################


if __name__ == "__main__":
  app = GUI()
  gdk.threads_enter()
  gtk.main()
  gdk.threads_leave()

