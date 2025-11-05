//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <algorithm>
#include <map>

static bool migrating = false;
//static unsigned active_machines = 16;
//tasks that need to still be done on sleeping machines
static std::map<MachineId_t, std::vector<TaskId_t>> pendingTasks;

struct VMInfoWrapper {
    VMId_t id;
    MachineId_t machine;
    CPUType_t cpu;
    unsigned numActiveTasks;
};

static Scheduler Scheduler;

double getUtilization(MachineId_t machineId)
{
    MachineInfo_t info = Machine_GetInfo(machineId);
    return (double) info.active_tasks/info.num_cpus;
}

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Initializing greedy scheduler", 1);

    unsigned totalMachines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + std::to_string(totalMachines), 3);

    for(unsigned i = 0; i < totalMachines; i++)
    {
        machines.push_back(MachineId_t(i));
    }

    for(unsigned i = 0; i < 16; i++)
    {
        if(Machine_GetCPUType(MachineId_t(i)) == X86)
        {
            VMId_t vm = VM_Create(LINUX, X86);
            vms.push_back(vm);
            VM_Attach(vm, MachineId_t(i));
        }
    }

    for(unsigned i = 16; i < totalMachines; i++)
    {
        Machine_SetState(MachineId_t(i), S5);
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    migrating = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    //  IsGPUCapable(task_id);
    //  GetMemory(task_id);
    //  RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or
    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    /*Priority_t priority = (task_id == 0 || task_id == 64)? HIGH_PRIORITY : MID_PRIORITY;
    if(migrating) {
        VM_AddTask(vms[0], task_id, priority);
    }
    else {
        VM_AddTask(vms[task_id % active_machines], task_id, priority);
    }// Skeleton code, you need to change it according to your algorithm*/

    CPUType_t CPUreq = RequiredCPUType(task_id);
    unsigned memNeeded = GetTaskMemory(task_id);
    SLAType_t slaType = RequiredSLA(task_id);

    Priority_t priority;
    switch (slaType)
    {
        case SLA0: priority = HIGH_PRIORITY; break;
        case SLA1: priority = MID_PRIORITY; break;
        case SLA2: priority = LOW_PRIORITY; break;
        case SLA3: priority = LOW_PRIORITY; break;
        default: priority = LOW_PRIORITY; break;
    }

    std::vector<MachineId_t> matchingMachines;
    for(MachineId_t machineId : machines)
    {
        MachineInfo_t info = Machine_GetInfo(machineId);
        //does order matter???
        if(info.s_state == S0 && Machine_GetCPUType(machineId) == CPUreq && info.memory_size - info.memory_used >= memNeeded)
        {
            matchingMachines.push_back(machineId);
        }
    }

    bool compareMachinesUtil(MachineId_t a, MachineId_t b)
    {
        return getUtilization(a) < getUtilization(b);
    }

    std::sort(matchingMachines.begin(), matchingMachines.end(), compareMachinesUtil);

    bool assigned = false;
    for (MachineId_t machineId : matchingMachines)
    {
        for (VMId_t vmId : vms)
        {
            VMInfo_t vmInfo = VM_GetInfo(vmId);
            if(vmInfo.machine_id == machineId && vmInfo.cpu == CPUreq)
            {
                VM_AddTask(vmId, task_id, priority);
                assigned = true;
                //SIMOUTPUT HERE 
                break;
            }
        }
        if(assigned)
        {
            break;
        } 
    }

    if(!assigned && !matchingMachines.empty()) 
    {
        VMId_t newVm = VM_Create(RequiredVMType(task_id), CPUreq);
        VM_Attach(newVm, matchingMachines[0]);
        VM_AddTask(newVm, task_id, priority);
        vms.push_back(newVm);
        assigned = true;
        //SimOutput("Created new VM " + std::to_string(newVm) + " for task " + std::to_string(task_id) + " on machine " + std::to_string(matchingMachines[0]), 2);
    }

    if(!assigned) 
    {
        for(MachineId_t machineId : machines) 
        {
            MachineInfo_t info = Machine_GetInfo(machineId);
            if(info.s_state == S5 && Machine_GetCPUType(machineId) == CPUreq) 
            {
                Machine_SetState(machineId, S0);
                pendingTasks[machineId].push_back(task_id);
                assigned = true;
                //SimOutput("Waking up machine " + std::to_string(machineId) + " for task " + std::to_string(task_id), 1);
                break;
            }
        }
    }

    if(!assigned) 
    {
        SimOutput("WARNING: Could not assign task " + std::to_string(task_id), 0);
    }
}


void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

// Public interface below

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    static unsigned counts = 0;
    counts++;
    if(counts == 10) {
        migrating = true;
        VM_Migrate(1, 9);
    }
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}

