//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "Interfaces.h"

class Scheduler {
public:
    Scheduler()                 {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
    void ChangeComplete(Time_t time, MachineId_t machine_id);
    void HandleMemoryWarning(Time_t now, MachineId_t machine_id);
private:
    vector<VMId_t> vms;
    vector<MachineId_t> machines;

    std::unordered_set<MachineId_t> quarantined;
    std::unordered_map<MachineId_t, Time_t> last_mem_warn;
};



#endif /* Scheduler_hpp */
