//
//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//


#include "Scheduler.hpp"
#include <map>
#include <set>
#include <vector>
#include <algorithm>


using std::map;
using std::set;
using std::vector;


static bool migrating = false;
static unsigned active_machines = 16;

struct VMRecord {
   set<TaskId_t> active_tasks;
   MachineId_t host_machine;
};


struct MachineRecord {
   set<VMId_t> vms;
   bool powered_on;
   CPUType_t cpu_type;
};


map<VMId_t, VMRecord> vm_map;
map<MachineId_t, MachineRecord> machine_map;
map<TaskId_t, VMId_t> task_to_vm_map;


vector<MachineId_t> machines;
vector<VMId_t> vms;
unsigned total_tasks = 0;
unsigned completed_tasks = 0;
const unsigned MAX_TASKS_PER_VM = 3;


MachineId_t FindBestMachine(TaskId_t task_id) {
   TaskInfo_t task_info = GetTaskInfo(task_id);
   MachineId_t best = -1;
   unsigned min_load = 1000;

   for (auto m_id : machines) {
       MachineInfo_t minfo = Machine_GetInfo(m_id);
       if (minfo.cpu != task_info.required_cpu) continue;


       if (!machine_map[m_id].powered_on) continue;


       unsigned load = minfo.active_tasks;
       if (load < min_load) {
           min_load = load;
           best = m_id;
       }
   }

   if (best == -1) {
       for (auto m_id : machines) {
           MachineInfo_t minfo = Machine_GetInfo(m_id);
           if (minfo.cpu == task_info.required_cpu && !machine_map[m_id].powered_on) {
               Machine_SetState(m_id, S0);
               machine_map[m_id].powered_on = true;
               best = m_id;
               break;
           }
       }
   }


   return best;
}




VMId_t FindAvailableVM(TaskId_t task_id) {
   TaskInfo_t t_info = GetTaskInfo(task_id);


   for (auto& [vm_id, vm] : vm_map) {
       if (vm.active_tasks.size() < MAX_TASKS_PER_VM) {
           MachineInfo_t minfo = Machine_GetInfo(vm.host_machine);
           if (minfo.cpu == t_info.required_cpu) return vm_id;
       }
   }
   return -1;
}


void AdjustMachinePerformance(MachineId_t m_id) {
   MachineInfo_t info = Machine_GetInfo(m_id);
   if (!machine_map[m_id].powered_on) return;


   double util = double(info.active_tasks) / info.num_cpus;
   if (util <= 0.25) Machine_SetCorePerformance(m_id, -1, P3);
   else if (util <= 0.5) Machine_SetCorePerformance(m_id, -1, P2);
   else if (util <= 0.75) Machine_SetCorePerformance(m_id, -1, P1);
   else Machine_SetCorePerformance(m_id, -1, P0);
}


void Scheduler::Init() {
   SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
   SimOutput("Scheduler::Init(): Initializing scheduler", 1);


   unsigned machine_total = Machine_GetTotal();

   for(unsigned i = 0; i < machine_total; i++) {
   MachineInfo_t minfo = Machine_GetInfo(i);   
   MachineId_t m_id = MachineId_t(i);

   VMType_t vm_type = LINUX;                   
   VMId_t vm_id = VM_Create(vm_type, minfo.cpu);

   VM_Attach(vm_id, m_id);


   machines.push_back(m_id);
   vms.push_back(vm_id);

   machine_map[m_id] = {{vm_id}, true, minfo.cpu};
   vm_map[vm_id] = {{}, m_id};
}
}
void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
   // Update your data structure. The VM now can receive new tasks
}


void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
   Priority_t priority = (task_id == 0 || task_id == 64)? HIGH_PRIORITY : MID_PRIORITY;


   //try to assign to existing VM first
   VMId_t vm_id = FindAvailableVM(task_id);
   if (vm_id != -1) {
       VM_AddTask(vm_id, task_id, priority);
       vm_map[vm_id].active_tasks.insert(task_id);
       task_to_vm_map[task_id] = vm_id;
       return;
   }


   //create new VM and attach to best machine
   MachineId_t m_id = FindBestMachine(task_id);
   if (m_id == -1) {
       ThrowException("No suitable machine for task " + std::to_string(task_id));
       return;
   }


   if (!machine_map[m_id].powered_on) {
       Machine_SetState(m_id, S0);
       machine_map[m_id].powered_on = true;
   }


   VMType_t vm_type = RequiredVMType(task_id);
   CPUType_t cpu_type = static_cast<CPUType_t>(RequiredCPUType(task_id));
   VMId_t new_vm = VM_Create(vm_type, cpu_type);


   VM_Attach(new_vm, m_id);
   machine_map[m_id].vms.insert(new_vm);
   vm_map[new_vm] = {{task_id}, m_id};
   task_to_vm_map[task_id] = new_vm;
   VM_AddTask(new_vm, task_id, priority);
   vms.push_back(new_vm);
}


void Scheduler::PeriodicCheck(Time_t now) {
   for (auto m_id : machines)
       AdjustMachinePerformance(m_id);


   //migrate low-priority VM if machine overloaded
   for (auto m_id : machines) {
       MachineInfo_t info = Machine_GetInfo(m_id);
       if (!machine_map[m_id].powered_on || info.active_tasks <= info.num_cpus) continue;


       VMId_t vm_to_migrate = -1;
       for (auto vm_id : machine_map[m_id].vms) {
           if (!vm_map[vm_id].active_tasks.empty()) {
               TaskId_t t_id = *vm_map[vm_id].active_tasks.begin();
               if (GetTaskPriority(t_id) == LOW_PRIORITY) {
                   vm_to_migrate = vm_id;
                   break;
               }
           }
       }


       if (vm_to_migrate != -1) {
           TaskId_t t_id = *vm_map[vm_to_migrate].active_tasks.begin();
           MachineId_t new_m = FindBestMachine(t_id);
           if (new_m != -1 && new_m != m_id) {
               VM_Migrate(vm_to_migrate, new_m);
               machine_map[m_id].vms.erase(vm_to_migrate);
               machine_map[new_m].vms.insert(vm_to_migrate);
               vm_map[vm_to_migrate].host_machine = new_m;
           }
       }
   }
}


void Scheduler::Shutdown(Time_t time) {
   //only shutdown VMs that are still active
   for (auto it = vms.begin(); it != vms.end();) {
       VMId_t vm = *it;
       if (vm_map.find(vm) != vm_map.end()) {
           VM_Shutdown(vm);
           it = vms.erase(it);
       } else {
           ++it; 
       }
   }
   SimOutput("SimulationComplete(): Finished!", 4);
   SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}


void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
   auto it = task_to_vm_map.find(task_id);
   if (it == task_to_vm_map.end()) return;


   VMId_t vm_id = it->second;
   vm_map[vm_id].active_tasks.erase(task_id);
   task_to_vm_map.erase(it);


   MachineId_t m_id = vm_map[vm_id].host_machine;
   if (vm_map[vm_id].active_tasks.empty()) {
       VM_Shutdown(vm_id);
       machine_map[m_id].vms.erase(vm_id);
       vm_map.erase(vm_id);
   }


   completed_tasks++;
   SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " complete at " + to_string(now), 4);
}


// Public interface below


static Scheduler Scheduler;


void InitScheduler()
{
   SimOutput("InitScheduler(): Initializing scheduler", 4);
   Scheduler.Init();
}


void HandleNewTask(Time_t time, TaskId_t task_id)
{
   SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
   Scheduler.NewTask(time, task_id);
}


void HandleTaskCompletion(Time_t time, TaskId_t task_id)
{
   SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
   Scheduler.TaskComplete(time, task_id);
}


void MemoryWarning(Time_t time, MachineId_t machine_id)
{
   // The simulator is alerting you that machine identified by machine_id is overcommitted
   SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}


void MigrationDone(Time_t time, VMId_t vm_id)
{
   // The function is called on to alert you that migration is complete
   SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
   Scheduler.MigrationComplete(time, vm_id);
}


void SchedulerCheck(Time_t time)
{
   // This function is called periodically by the simulator, no specific event
   SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
   Scheduler.PeriodicCheck(time);
   // static unsigned counts = 0;
   // counts++;
   // if(counts == 10) {
   //     migrating = true;
   //     VM_Migrate(1, 9);
   // }
}


void SimulationComplete(Time_t time)
{
   // This function is called before the simulation terminates Add whatever you feel like.
   cout << "SLA violation report" << endl;
   cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
   cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
   cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl; // SLA3 do not have SLA violation issues
   cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
   cout << "Simulation run finished in " << double(time) / 1000000 << " seconds" << endl;
   SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);


   Scheduler.Shutdown(time);
}


void SLAWarning(Time_t time, TaskId_t task_id)
{
   SetTaskPriority(task_id, HIGH_PRIORITY);
}


void StateChangeComplete(Time_t time, MachineId_t machine_id)
{
   // Called in response to an earlier request to change the state of a machine
}



