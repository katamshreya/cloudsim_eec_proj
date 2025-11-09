//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//


#ifndef Scheduler_hpp
#define Scheduler_hpp


#include <vector>
#include <unordered_map>
#include "Interfaces.h"
#include <map>


class Scheduler {
public:
   Scheduler()                 {}
   void Init();
   void MigrationComplete(Time_t time, VMId_t vm_id);
   void NewTask(Time_t now, TaskId_t task_id);
   void PeriodicCheck(Time_t now);
   void Shutdown(Time_t now);
   void TaskComplete(Time_t now, TaskId_t task_id);
private:
   struct PendingVM {
       VMType_t vmType;
       CPUType_t cpu;
       TaskId_t taskId;
       Priority_t priority;
   };
   vector<VMId_t> vms;
   vector<MachineId_t> machines;
   std::unordered_map<MachineId_t, unsigned> machineLoad;
   std::unordered_map<TaskId_t, VMId_t> taskToVM;
   map<MachineId_t, PendingVM> pendingVMs;




};

#endif /* Scheduler_hpp */


