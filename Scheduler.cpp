//
//  Scheduler.cpp
//  CloudSim
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
    // VM/CPU compatibility policy
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

// Try to place as many queued tasks for this virtual class as capacity allows.
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

        // simple scan: pick the first VM whose host can take this task now
        for (VMId_t vm : vms) {
            VMInfo_t vi = VM_GetInfo(vm);
            MachineInfo_t mi = Machine_GetInfo(vi.machine_id);
            if (fitsTask(mi, ti)) { chosen = vm; break; }
        }

        if (chosen == VMId_t(-1)) break; // nothing can take it now

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

    // 1) Group hosts directly by struct MachineKey for a small subset of states
    //    We seed only S0/S0i1/S1/S2 buckets; deeper sleepers are left alone at init.
    for (unsigned i = 0; i < total; ++i) {
        MachineId_t mid = MachineId_t(i);
        MachineInfo_t mi = Machine_GetInfo(mid);
        allMachines.push_back(mid);

        if (mi.s_state == S0 || mi.s_state == S0i1 || mi.s_state == S1 || mi.s_state == S2) {
            MachineKey mk{mi.s_state, mi.cpu, mi.gpus};
            hostBuckets[mk].push_back(mid);
        }
    }

    // 2) Pre-create a VM for each host bucket using the host CPU’s default VM type.
    //    Also create SLA-variant virtual buckets to route tasks later.
    for (const auto& hb : hostBuckets) {
        const MachineKey& mk = hb.first;
        const auto& mids = hb.second;

        for (MachineId_t mid : mids) {
            MachineInfo_t mi = Machine_GetInfo(mid);
            if (mi.s_state != S0) continue; // attach only to fully awake hosts

            VMType_t vmType = defaultVM(mi.cpu);
            VMId_t vm = VM_Create(vmType, mi.cpu);
            VM_Attach(vm, mid);

            // create 4 virtual classes for the same vmType/CPU/gpu, one per SLA
            for (int s = SLA0; s <= SLA3; ++s) {
                VirtualKey vk{vmType, static_cast<SLAType_t>(s), mi.cpu, mk.gpu};
                vmBuckets[vk].push_back(vm);
                // queues map will auto-create on demand
            }
        }
    }

    SimOutput("Scheduler::Init(): Buckets: hosts=" + to_string(hostBuckets.size()) +
              ", vmBuckets=" + to_string(vmBuckets.size()), 2);
}

void Scheduler::MigrationComplete(Time_t /*time*/, VMId_t /*vm_id*/) {
    // No special bookkeeping in this variant
}

void Scheduler::NewTask(Time_t /*now*/, TaskId_t task_id) {
    TaskInfo_t info = GetTaskInfo(task_id);

    // Preferred class
    VirtualKey vk{info.required_vm, info.required_sla, info.required_cpu, info.gpu_capable};

    auto try_enqueue = [&](const VirtualKey& key) -> bool {
        // ensure a VM pool exists; if not, see if we can build one on an S0 host of matching profile
        auto vIt = vmBuckets.find(key);
        if (vIt == vmBuckets.end() || vIt->second.empty()) {
            // attempt to create one lazily on any S0 host with matching cpu/gpu
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
        // finally enqueue and try to drain
        queues[key].push(task_id);
        drainQueue(key);
        return true;
    };

    // Route to preferred class if possible
    if (try_enqueue(vk)) return;

    // Fallback 1: same everything but flip GPU capability if that class exists
    VirtualKey altGPU{vk.vm, vk.sla, vk.cpu, !vk.gpu};
    if (vmBuckets.count(altGPU)) { queues[altGPU].push(task_id); drainQueue(altGPU); return; }

    // Fallback 2: relax SLA downward until SLA0
    for (int s = static_cast<int>(vk.sla) - 1; s >= static_cast<int>(SLA0); --s) {
        VirtualKey relaxed{vk.vm, static_cast<SLAType_t>(s), vk.cpu, vk.gpu};
        if (vmBuckets.count(relaxed)) { queues[relaxed].push(task_id); drainQueue(relaxed); return; }
    }

    // Last resort: queue on preferred to surface pressure; it may drain later as capacity frees up
    queues[vk].push(task_id);
}

void Scheduler::PeriodicCheck(Time_t /*now*/) {
    // Opportunistically drain all non-empty queues
    for (auto& kv : queues) {
        if (!kv.second.empty()) drainQueue(kv.first);
    }

    // Optional energy tidy: power down idle S0 hosts with no VMs or tasks
    for (MachineId_t mid : allMachines) {
        MachineInfo_t mi = Machine_GetInfo(mid);
        if (mi.s_state == S0 && mi.active_tasks == 0 && mi.active_vms == 0) {
            Machine_SetState(mid, S5);
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    // Gather unique VMs because the same VM appears in multiple vmBuckets (one per SLA).
    std::unordered_set<VMId_t> unique_vms;
    for (auto &entry : vmBuckets) {
        for (VMId_t vm : entry.second) {
            unique_vms.insert(vm);
        }
    }

    // Shut down each VM at most once, and only if its host isn't already off.
    for (VMId_t vm : unique_vms) {
        VMInfo_t vi = VM_GetInfo(vm);

        // If the simulator marks detached/inactive VMs with an invalid machine_id, skip those.
        // Otherwise, check the host state before calling VM_Shutdown.
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

// ----------------- Public interface stubs -----------------
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
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    SchedulerSingleton.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    SchedulerSingleton.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
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
    // no-op in this variant
}
