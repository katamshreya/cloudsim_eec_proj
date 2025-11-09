//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <climits>

static bool migrating = false;

void Scheduler::Init()
{
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    unsigned total_Machines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    for (unsigned i = 0; i < total_Machines; i++)
    {
        machines.push_back(MachineId_t(i));
        machineLoad[machines[i]] = 0;
        if (Machine_GetCPUType(MachineId_t(i)) != X86)
        {
            Machine_SetState(MachineId_t(i), S5);
        }
        SimOutput("Scheduler::Init(): Initialization complete with " + to_string(total_Machines) + " machines.", 3);
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id)
{
    // Update your data structure. The VM now can receive new tasks
    migrating = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id)
{
    SimOutput("NewTask(): Attempting to schedule task " + to_string(task_id), 4);
    CPUType_t cpu = RequiredCPUType(task_id);
    unsigned memory = GetTaskMemory(task_id);
    VMType_t vmType = RequiredVMType(task_id);
    SLAType_t sla = RequiredSLA(task_id);

    // Determine priority using if-else
    Priority_t priority;
    if (sla == SLA0 || sla == SLA1)
    {
        priority = HIGH_PRIORITY;
    }
    else if (sla == SLA2)
    {
        priority = MID_PRIORITY;
    }
    else if (sla == SLA3)
    {
        priority = LOW_PRIORITY;
    }
    else
    {
        priority = MID_PRIORITY;
    }

    bool taskAssigned = false;

    // Greedy: assign to VM with max free memory
    VMId_t selectedVM = UINT_MAX;
    unsigned highestFreeMemory = 0;

    for (VMId_t vm : vms)
    {
        VMInfo_t info = VM_GetInfo(vm);
        MachineInfo_t mInfo = Machine_GetInfo(info.machine_id);
        unsigned freeMem = mInfo.memory_size - mInfo.memory_used;

        bool vmFit = (info.vm_type == vmType) && (info.cpu == cpu) &&
                     (mInfo.s_state == S0) && (freeMem >= memory);
        if (vmFit && freeMem > highestFreeMemory)
        {
            selectedVM = vm;
            highestFreeMemory = freeMem;
        }
    }

    if (selectedVM != UINT_MAX && highestFreeMemory > 0)
    {
        VM_AddTask(selectedVM, task_id, priority);
        taskToVM[task_id] = selectedVM;
        SimOutput("NewTask(): Task " + to_string(task_id) + " assigned to VM " + to_string(selectedVM), 2);
        MachineId_t m = VM_GetInfo(selectedVM).machine_id;
        machineLoad[m] += 1;

        taskAssigned = true;
    }

    std::unordered_map<MachineId_t, unsigned> machineActiveTasks;
    for (MachineId_t m : machines)
        machineActiveTasks[m] = 0;
    for (VMId_t vm : vms)
    {
        const VMInfo_t &info = VM_GetInfo(vm);
        machineActiveTasks[info.machine_id] += info.active_tasks.size();
    }

    // Assign to least loaded powered-on machine
    if (!taskAssigned)
    {
        MachineId_t leastLoaded = UINT_MAX;
        unsigned minTasks = UINT_MAX;

        for (MachineId_t m : machines)
        {
            MachineInfo_t mInfo = Machine_GetInfo(m);
            unsigned freeMem = mInfo.memory_size - mInfo.memory_used;

            if (mInfo.s_state == S0 && mInfo.cpu == cpu && freeMem >= memory)
            {
                unsigned activeTasks = 0;
                for (VMId_t vm : vms)
                {
                    VMInfo_t info = VM_GetInfo(vm);
                    if (info.machine_id == m)
                        activeTasks += info.active_tasks.size();
                }
                if (activeTasks < minTasks)
                {
                    leastLoaded = m;
                    minTasks = activeTasks;
                }
            }
        }

        if (leastLoaded != UINT_MAX)
        {
            VMId_t newVM = VM_Create(vmType, cpu);
            VM_Attach(newVM, leastLoaded);
            VM_AddTask(newVM, task_id, priority);
            taskToVM[task_id] = newVM;
            vms.push_back(newVM);
            SimOutput("NewTask(): Created VM " + to_string(newVM) + " on least loaded machine " + to_string(leastLoaded), 2);
            machineLoad[leastLoaded] += 1;
            taskAssigned = true;
        }
    }

    // Power on sleeping machine if still unassigned
    if (!taskAssigned)
    {
        for (MachineId_t m : machines)
        {
            MachineInfo_t mInfo = Machine_GetInfo(m);
            if (mInfo.s_state == S5 && mInfo.cpu == cpu && (mInfo.memory_size - mInfo.memory_used) >= memory)
            {
                Machine_SetState(m, S0);
                pendingVMs[m] = {vmType, cpu, task_id, priority};
                SimOutput("NewTask(): Powered on machine " + std::to_string(m) + " and pending VM for task " + std::to_string(task_id), 1);
                taskAssigned = true;
                break;
            }
        }
    }

    if (!taskAssigned)
        SimOutput("NewTask(): Task " + to_string(task_id) + " could not be assigned", 0);
}

void Scheduler::PeriodicCheck(Time_t now)
{
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    SimOutput("PeriodicCheck(): Running system scan at time " + to_string(now), 3);

    // Power down idle machines
    for (MachineId_t m : machines)
    {
        MachineInfo_t minfo = Machine_GetInfo(m);
        if (minfo.s_state != S0)
            continue;
        if (machineLoad[m] == 0)
        {
            Machine_SetState(m, S5);
            SimOutput("PeriodicCheck(): Powered down idle machine " + std::to_string(m), 2);
        }
    }
}

void Scheduler::Shutdown(Time_t time)
{
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for (VMId_t vm : vms)
    {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id)
{
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);

    if (taskToVM.find(task_id) != taskToVM.end())
    {
        VMId_t vm = taskToVM[task_id];
        MachineId_t m = VM_GetInfo(vm).machine_id;
        machineLoad[m]--;
        taskToVM.erase(task_id);

        if (Machine_GetInfo(m).s_state == S0 && machineLoad[m] == 0)
        {
            Machine_SetState(m, S5);
            SimOutput("TaskComplete(): Machine " + std::to_string(m) + " powered off (idle)", 2);
        }
    }
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
    migrating = false;
}

void SchedulerCheck(Time_t time)
{
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
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
}

void StateChangeComplete(Time_t time, MachineId_t machine_id)
{
    // Called in response to an earlier request to change the state of a machine
}
