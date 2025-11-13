//
//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <climits>
#include <cfloat>
#include <cmath>

static bool migrating = false;

void Scheduler::Init()
{
    unsigned total_machines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total machines = " + 
        
        to_string(total_machines), 2);

    for (unsigned i = 0; i < total_machines; i++)
    {
        MachineInfo_t m_info = Machine_GetInfo(i);

        if (m_info.s_state != S0)
            continue;

        machines.push_back(i);
        VMId_t vm_linux = VM_Create(LINUX, m_info.cpu);
        VM_Attach(vm_linux, i);
        vms.push_back(vm_linux);

        if (m_info.cpu == X86 && m_info.gpus)
        {
            VMId_t vm_linux_rt = VM_Create(LINUX_RT, m_info.cpu);
            VM_Attach(vm_linux_rt, i);
            vms.push_back(vm_linux_rt);
        }

        if (m_info.cpu == POWER)
        {
            VMId_t vm_aix = VM_Create(AIX, m_info.cpu);
            VM_Attach(vm_aix, i);
            vms.push_back(vm_aix);
        }

        SimOutput("Scheduler::Init(): Total active machines: " + 
            to_string(machines.size()), 2);
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id)
{
    migrating = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id)
{
    TaskInfo_t task_info = GetTaskInfo(task_id);
    Priority_t priority;
    if (task_info.required_sla == SLA0 || task_info.required_sla == SLA1)
        priority = HIGH_PRIORITY;
    else if (task_info.required_sla == SLA2)
        priority = MID_PRIORITY;
    else
        priority = LOW_PRIORITY;

    static size_t vm_rr = 0;      // Round-robin pointer for VMs
    static size_t machine_rr = 0; // Round-robin pointer for machines
    VMId_t selected_vm = VMId_t(-1);

    unsigned total_vms = vms.size();
    unsigned total_machines = Machine_GetTotal();

    //existing vms
    for (size_t i = 0; i < total_vms; ++i)
    {
        VMId_t vm_id = vms[(vm_rr + i) % total_vms];
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        MachineInfo_t m_info = Machine_GetInfo(vm_info.machine_id);

        if (m_info.s_state != S0 || vm_info.vm_type != task_info.required_vm 
            || m_info.cpu != task_info.required_cpu)
            continue;

        unsigned available_memory = m_info.memory_size - m_info.memory_used;
        if (available_memory < task_info.required_memory)
            continue;

        selected_vm = vm_id;
        vm_rr = (vm_rr + i + 1) % total_vms;
        VM_AddTask(selected_vm, task_id, priority);
        return;
    }

    //active machines, place VM
    for (size_t i = 0; i < total_machines; ++i)
    {
        MachineId_t machine_id = MachineId_t((machine_rr + i) % total_machines);
        MachineInfo_t m_info = Machine_GetInfo(machine_id);

        if (m_info.s_state != S0 || m_info.cpu != task_info.required_cpu)
            continue;
        unsigned available_memory = m_info.memory_size - m_info.memory_used;
        if (available_memory < task_info.required_memory)
            continue;

        // Check for existing VM on this machine
        bool found_vm = false;
        for (VMId_t vm_id : vms)
        {
            VMInfo_t vm_info = VM_GetInfo(vm_id);
            if (vm_info.machine_id == machine_id && vm_info.vm_type 
                == task_info.required_vm)
            {
                selected_vm = vm_id;
                found_vm = true;
                break;
            }
        }

        // Create VM if not found
        if (!found_vm)
        {
            selected_vm = VM_Create(task_info.required_vm, 
                task_info.required_cpu);
            VM_Attach(selected_vm, machine_id);
            vms.push_back(selected_vm);
            SimOutput("NewTask(): Created VM " + to_string(selected_vm) +
                          " on active machine " + to_string(machine_id),
                      2);
        }

        VM_AddTask(selected_vm, task_id, priority);
        machine_rr = (machine_rr + i + 1) % total_machines;
        return;
    }

    // Wake sleeping machine
    for (size_t i = 0; i < total_machines; ++i)
    {
        MachineId_t machine_id = MachineId_t((machine_rr + i) % total_machines);
        MachineInfo_t m_info = Machine_GetInfo(machine_id);

        if (m_info.s_state != S5 || m_info.cpu != task_info.required_cpu)
            continue;
        unsigned available_memory = m_info.memory_size - m_info.memory_used;
        if (available_memory < task_info.required_memory + VM_MEMORY_OVERHEAD)
            continue;

        //on   
        Machine_SetState(machine_id, S0);

        selected_vm = VM_Create(task_info.required_vm, task_info.required_cpu);
        VM_Attach(selected_vm, machine_id);
        VM_AddTask(selected_vm, task_id, priority);
        vms.push_back(selected_vm);

        machine_rr = (machine_rr + i + 1) % total_machines;
        SimOutput("NewTask(): Powered on sleeping machine " + 
            to_string(machine_id) +
                      " for task " + to_string(task_id),
                  2);
        return;
    }
    SimOutput("NewTask(): No placement found for task " + 
        to_string(task_id), 1);
}


void Scheduler::PeriodicCheck(Time_t now)
{
    vector<MachineId_t> idleMachines;
    for (MachineId_t machine : machines)
    {
        MachineInfo_t m_info = Machine_GetInfo(machine);
        if (m_info.s_state != S0)
            continue;
        if (m_info.active_tasks == 0 && m_info.active_vms == 0)
            idleMachines.push_back(machine);
    }

    for (MachineId_t idle : idleMachines)
    {
        Machine_SetState(idle, S5);
        SimOutput("PeriodicCheck(): Powered down idle machine " + to_string(idle), 2);
    }
}

void Scheduler::Shutdown(Time_t time)
{
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for (auto &vm : vms)
    {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
    SimOutput("Total Energy: " + to_string(Machine_GetClusterEnergy()) + " KW-Hour", 1);
    SimOutput("SLA0: " + to_string(GetSLAReport(SLA0)) + "%", 1);
    SimOutput("SLA1: " + to_string(GetSLAReport(SLA1)) + "%", 1);
    SimOutput("SLA2: " + to_string(GetSLAReport(SLA2)) + "%", 1);
    SimOutput("SLA3: best-effort", 1);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id)
{
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy

    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
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
