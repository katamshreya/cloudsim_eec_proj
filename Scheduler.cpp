//
//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <algorithm>
#include <unordered_set>

static bool migrating = false;

VMType_t Scheduler::defaultVM(CPUType_t cpu) {
    switch (cpu) {
        case X86:   return LINUX;
        case POWER: return AIX;
        case ARM:   return WIN;
        case RISCV: return LINUX;
        default:    return LINUX;
    }
}
Priority_t Scheduler::prioFromSLA(SLAType_t sla) {
    switch (sla) {
        case SLA0: return HIGH_PRIORITY;
        case SLA1: return HIGH_PRIORITY;
        case SLA2: return MID_PRIORITY;
        case SLA3: default: return LOW_PRIORITY;
    }
}
bool Scheduler::canHostVM(const MachineInfo_t& mi, VMType_t vmType, CPUType_t vmCpu) {
    if (vmType == AIX && mi.cpu != POWER) return false;
    if (vmType == WIN && !(mi.cpu == ARM || mi.cpu == X86)) return false;
    return mi.cpu == vmCpu;
}
bool Scheduler::fitsTask(const MachineInfo_t& mi, const TaskInfo_t& ti) {
    if (mi.s_state != S0) return false;
    if (mi.active_tasks >= mi.num_cpus) return false;
    if (mi.memory_used + ti.required_memory > mi.memory_size) return false;
    return true;
}

void Scheduler::drainQueue(const VirtualKey& key) {
    auto qIt = queues.find(key);
    if (qIt == queues.end()) return;

    auto vIt = vmBuckets.find(key);
    if (vIt == vmBuckets.end()) return;

    auto& q  = qIt->second;
    auto& vms = vIt->second;

    while (!q.empty()) {
        TaskId_t tid = q.front();
        TaskInfo_t ti = GetTaskInfo(tid);

        VMId_t chosen = VMId_t(-1);

        for (VMId_t vm : vms) {
            VMInfo_t vi = VM_GetInfo(vm);
            MachineInfo_t mi = Machine_GetInfo(vi.machine_id);
            if (fitsTask(mi, ti)) { chosen = vm; break; }
        }

        if (chosen == VMId_t(-1)) break;

        q.pop();
        VM_AddTask(chosen, tid, prioFromSLA(ti.required_sla));
    }
}

void Scheduler::Init() {
    unsigned total = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(total), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler (struct-key buckets)", 1);

    hostBuckets.clear();
    vmBuckets.clear();
    queues.clear();
    allMachines.clear();
    allMachines.reserve(total);

    for (unsigned i = 0; i < total; ++i) {
        MachineId_t mid = MachineId_t(i);
        MachineInfo_t mi = Machine_GetInfo(mid);
        allMachines.push_back(mid);

        if (mi.s_state == S0 || mi.s_state == S0i1 || mi.s_state == S1 || mi.s_state == S2) {
            MachineKey mk{mi.s_state, mi.cpu, mi.gpus};
            hostBuckets[mk].push_back(mid);
        }
    }

    for (const auto& hb : hostBuckets) {
        const MachineKey& mk = hb.first;
        const auto& mids = hb.second;

        for (MachineId_t mid : mids) {
            MachineInfo_t mi = Machine_GetInfo(mid);
            if (mi.s_state != S0) continue;

            VMType_t vmType = defaultVM(mi.cpu);
            VMId_t vm = VM_Create(vmType, mi.cpu);
            VM_Attach(vm, mid);

            for (int s = SLA0; s <= SLA3; ++s) {
                VirtualKey vk{vmType, static_cast<SLAType_t>(s), mi.cpu, mk.gpu};
                vmBuckets[vk].push_back(vm);
            }
        }
    }

    SimOutput("Scheduler::Init(): Buckets: hosts=" + to_string(hostBuckets.size()) +
              ", vmBuckets=" + to_string(vmBuckets.size()), 2);
}

void Scheduler::MigrationComplete(Time_t /*time*/, VMId_t /*vm_id*/) {
    // Update your data structure. The VM now can receive new tasks
}

void Scheduler::NewTask(Time_t /*now*/, TaskId_t task_id) {
    TaskInfo_t info = GetTaskInfo(task_id);

    VirtualKey vk{info.required_vm, info.required_sla, info.required_cpu, info.gpu_capable};

    auto try_enqueue = [&](const VirtualKey& key) -> bool {
        auto vIt = vmBuckets.find(key);
        if (vIt == vmBuckets.end() || vIt->second.empty()) {
            for (const auto& hb : hostBuckets) {
                const MachineKey& mk = hb.first;
                if (mk.state != S0) continue;
                if (mk.cpu != key.cpu || mk.gpu != key.gpu) continue;
                for (MachineId_t mid : hb.second) {
                    MachineInfo_t mi = Machine_GetInfo(mid);
                    if (mi.s_state != S0) continue;
                    if (!canHostVM(mi, key.vm, key.cpu)) continue;

                    VMId_t vm = VM_Create(key.vm, key.cpu);
                    VM_Attach(vm, mid);
                    vmBuckets[key].push_back(vm);
                    goto created;
                }
            }
        }
    created:
        queues[key].push(task_id);
        drainQueue(key);
        return true;
    };

    if (try_enqueue(vk)) return;

    VirtualKey altGPU{vk.vm, vk.sla, vk.cpu, !vk.gpu};
    if (vmBuckets.count(altGPU)) { queues[altGPU].push(task_id); drainQueue(altGPU); return; }

    for (int s = static_cast<int>(vk.sla) - 1; s >= static_cast<int>(SLA0); --s) {
        VirtualKey relaxed{vk.vm, static_cast<SLAType_t>(s), vk.cpu, vk.gpu};
        if (vmBuckets.count(relaxed)) { queues[relaxed].push(task_id); drainQueue(relaxed); return; }
    }

    queues[vk].push(task_id);
}

void Scheduler::PeriodicCheck(Time_t /*now*/) {
    for (auto& kv : queues) {
        if (!kv.second.empty()) drainQueue(kv.first);
    }

    for (MachineId_t mid : allMachines) {
        MachineInfo_t mi = Machine_GetInfo(mid);
        if (mi.s_state == S0 && mi.active_tasks == 0 && mi.active_vms == 0) {
            Machine_SetState(mid, S5);
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    std::unordered_set<VMId_t> unique_vms;
    for (auto &entry : vmBuckets) {
        for (VMId_t vm : entry.second) {
            unique_vms.insert(vm);
        }
    }

    for (VMId_t vm : unique_vms) {
        VMInfo_t vi = VM_GetInfo(vm);
        if (static_cast<int>(vi.machine_id) >= 0) {
            MachineInfo_t mi = Machine_GetInfo(vi.machine_id);
            if (mi.s_state != S5) {
                VM_Shutdown(vm);
            }
        }
    }

    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

// Public interface below
static Scheduler SchedulerSingleton;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    SchedulerSingleton.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    SchedulerSingleton.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    SchedulerSingleton.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    SchedulerSingleton.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    SchedulerSingleton.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at " + to_string(time), 4);

    SchedulerSingleton.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    TaskInfo_t ti = GetTaskInfo(task_id);
    Priority_t p  = ti.priority;
    if (p != HIGH_PRIORITY) p = static_cast<Priority_t>(static_cast<unsigned>(p) - 1u);
    SetTaskPriority(task_id, p);
}

void StateChangeComplete(Time_t /*time*/, MachineId_t /*machine_id*/) {
    // Called in response to an earlier request to change the state of a machine
}
