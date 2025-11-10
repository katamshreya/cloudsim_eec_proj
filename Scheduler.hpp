//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//


#ifndef Scheduler_hpp
#define Scheduler_hpp


#include <vector>
#include <queue>


#include "Interfaces.h"
#include <unordered_map>
#include <set>



class Scheduler {
public:
   Scheduler()                 {}
   void Init();
   void MigrationComplete(Time_t time, VMId_t vm_id);
   void NewTask(Time_t now, TaskId_t task_id);
   void PeriodicCheck(Time_t now);
   void Shutdown(Time_t now);
   void TaskComplete(Time_t now, TaskId_t task_id);
   void StateChangeComplete(Time_t now, MachineId_t machine_id);

private:
   vector<VMId_t> vms;
   vector<MachineId_t> machines;
   VMType_t GetDefaultVMForCPU(CPUType_t cpu_type);
   std::vector<TaskId_t> pending_tasks;

};






#endif /* Scheduler_hpp */

