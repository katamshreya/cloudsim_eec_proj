//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include "Interfaces.h"
#include "SimTypes.h"
#include <algorithm>
#include <vector>
#include <cfloat>
#include <cmath>
#include <unordered_map>
using std::unordered_map;
static unordered_map<MachineId_t, vector<TaskId_t>> pending; 

using std::vector;

static bool migrating = false;
static unsigned active_machines = 16;

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    
    //waking the best machines based on ranking efficiency
    vector<MachineId_t> all;
    all.reserve(Machine_GetTotal());
    for(unsigned i = 0; i < Machine_GetTotal(); i++)
    {
        all.push_back(MachineId_t(i));
    }

    //used ai to figure out computation
    std::sort(all.begin(), all.end(), [](MachineId_t a, MachineId_t b)
    {
        auto A = Machine_GetInfo(a), B = Machine_GetInfo(b);
        auto cpuA = (A.num_cpus ? double(A.active_tasks)/double(A.num_cpus) : 0.0);
        auto memA = (A.memory_size ? double(A.memory_used)/double(A.memory_size) : 0.0);
        auto cpuB = (B.num_cpus ? double(B.active_tasks)/double(B.num_cpus) : 0.0);
        auto memB = (B.memory_size ? double(B.memory_used)/double(B.memory_size) : 0.0);
        double sA = 0.6*cpuA + 0.4*memA - (A.s_state != S0 ? 0.3*int(A.s_state) : 0.0);
        double sB = 0.6*cpuB + 0.4*memB - (B.s_state != S0 ? 0.3*int(B.s_state) : 0.0);
        return sA > sB;
    });

    unsigned awake = 0;
    for (auto id : all) 
    {
        MachineInfo_t mi = Machine_GetInfo(id);
        if (awake < active_machines) 
        {
            if (mi.s_state != S0) 
            {
                Machine_SetState(id, S0);
            }
            VMType_t vmtype = (mi.cpu == POWER ? AIX : LINUX);
            VMId_t vm = VM_Create(vmtype, mi.cpu);
            VM_Attach(vm, id);
            vms.push_back(vm);
            machines.push_back(id);
            awake++;
        } 
        else if (mi.active_tasks == 0 && mi.s_state != S3) 
        {
            Machine_SetState(id, S3);
        }
    }

    if (vms.size() >= 2)
    {
        SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " + to_string(vms[1]), 3);
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    TaskInfo_t ti = GetTaskInfo(task_id);

    auto fits = [&](const MachineInfo_t& m)->bool
    {
        double c = m.num_cpus ? double(m.active_tasks + 1)/double(m.num_cpus) : 1.0;
        double u = m.memory_size? double(m.memory_used + ti.required_memory)/double(m.memory_size) : 1.0;
        return c <= 0.80 && u <= 0.85;
    };

    auto score = [&](MachineId_t mid)->double
    {
        auto m = Machine_GetInfo(mid);
        double cpu = m.num_cpus ? double(m.active_tasks)/double(m.num_cpus) : 0.0;
        double mem = m.memory_size? double(m.memory_used)/double(m.memory_size) : 0.0;
        double s = 0.6*cpu + 0.4*mem - (m.s_state != S0 ? 0.3*int(m.s_state) : 0.0);
        return s;
    };

    //waking best host first
    vector<MachineId_t> awake = machines;
    std::sort(awake.begin(), awake.end(), [&](MachineId_t a, MachineId_t b){ return score(a) > score(b); });

    for (auto mid : awake) 
    {
        if (quarantined.count(mid)) continue;
        MachineInfo_t m = Machine_GetInfo(mid);
        if (m.s_state != S0) continue;
        if (Machine_GetCPUType(mid) != ti.required_cpu) continue;
        if (!fits(m)) continue;

        //reuse VM if present
        for (auto vm : vms) {
            VMInfo_t vi = VM_GetInfo(vm);
            if (vi.machine_id == mid && vi.vm_type == ti.required_vm) 
            {
                VM_AddTask(vm, task_id, ti.priority);
                return;
            }
        }
        //otherwise create one
        VMId_t vm = VM_Create(ti.required_vm, ti.required_cpu);
        VM_Attach(vm, mid);
        vms.push_back(vm);
        VM_AddTask(vm, task_id, ti.priority);
        return;
    }

    //wake up best sleeping 
    vector<MachineId_t> sleepers;
    for (unsigned i = 0; i < Machine_GetTotal(); ++i) 
    {
        MachineId_t id = MachineId_t(i);
        if (Machine_GetCPUType(id) == ti.required_cpu && Machine_GetInfo(id).s_state != S0)
        {
            sleepers.push_back(id);
        }
    }
    std::sort(sleepers.begin(), sleepers.end(), [&](MachineId_t a, MachineId_t b){ return score(a) > score(b); });

    for (auto mid : sleepers) 
    {
        MachineInfo_t m = Machine_GetInfo(mid);
        if (m.memory_size && (m.memory_used + ti.required_memory >= m.memory_size)) continue;

        pending[mid].push_back(task_id);
        Machine_SetState(mid, S0);
        return;
    }

    //lastly shortest queue 
    VMId_t best = VMId_t(-1);
    size_t fewest = SIZE_MAX;
    for (auto vm : vms) 
    {
        VMInfo_t vi = VM_GetInfo(vm);
        if (vi.cpu != ti.required_cpu) continue;
        if (vi.active_tasks.size() < fewest) { fewest = vi.active_tasks.size(); best = vm; }
    }
    if (best != VMId_t(-1)) 
    {
        VM_AddTask(best, task_id, ti.priority);
        return;
    }

    SimOutput("NewTask(): No placement found for task " + to_string(task_id), 1);
}

void Scheduler::PeriodicCheck(Time_t now) {
    for (auto mid : machines) 
    {
        MachineInfo_t m = Machine_GetInfo(mid);
        if (m.s_state != S0) continue;
        double u = m.num_cpus ? double(m.active_tasks)/double(m.num_cpus) : 1.0;
        double memu = m.memory_size ? double(m.memory_used)/double(m.memory_size) : 0.0;
        if (memu < 0.75) quarantined.erase(mid);
        CPUPerformance_t p = (u > 0.70) ? P0 : (u > 0.40) ? P1 : (u > 0.20) ? P2 : P3;
        for (unsigned c = 0; c < m.num_cpus; ++c) Machine_SetCorePerformance(mid, c, p);
    }
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
    //check every 400 tasks
    static unsigned seen = 0;
    if (++seen % 400 != 0) return;

    TaskInfo_t ti = GetTaskInfo(task_id);

    // Build awake host list for same CPU
    vector<MachineId_t> hosts;
    for (auto mid : machines) 
    {
        if (Machine_GetCPUType(mid) == ti.required_cpu && Machine_GetInfo(mid).s_state == S0)
        {
            hosts.push_back(mid);
        } 
    }
    if (hosts.size() < 2) return;

    //sort by combined util low to high
    std::sort(hosts.begin(), hosts.end(), [](MachineId_t a, MachineId_t b){
        auto A = Machine_GetInfo(a), B = Machine_GetInfo(b);
        double ua = (A.num_cpus ? double(A.active_tasks)/double(A.num_cpus) : 0.0)
                  + (A.memory_size? double(A.memory_used)/double(A.memory_size) : 0.0);
        double ub = (B.num_cpus ? double(B.active_tasks)/double(B.num_cpus) : 0.0)
                  + (B.memory_size? double(B.memory_used)/double(B.memory_size) : 0.0);
        return ua < ub;
    });

    auto vm_bytes = [](const VMInfo_t& vi)->size_t
    {
        size_t mb = 0; for (auto t: vi.active_tasks) mb += GetTaskInfo(t).required_memory;
        return mb ? mb : 100;
    };

    //pick smallest VM on low half
    VMId_t vm_pick = VMId_t(-1);
    MachineId_t src = MachineId_t(-1);
    size_t bytes_min = SIZE_MAX;
    size_t half = hosts.size()/2;

    for (size_t i = 0; i < half; ++i) 
    {
        MachineId_t mid = hosts[i];
        for (auto vm : vms) 
        {
            VMInfo_t vi = VM_GetInfo(vm);
            if (vi.machine_id != mid || vi.active_tasks.empty()) continue;
            size_t mb = vm_bytes(vi);
            if (mb < bytes_min) { bytes_min = mb; vm_pick = vm; src = mid; }
        }
    }
    if (vm_pick == VMId_t(-1)) return;

    VMInfo_t vinfo = VM_GetInfo(vm_pick);

    //higher-util host if it fits and saves power
    for (size_t j = hosts.size(); j-- > half; ) 
    {
        MachineId_t dst = hosts[j];
        if (dst == src) continue;
        MachineInfo_t dm = Machine_GetInfo(dst);

        //capacity after move
        double c_after = dm.num_cpus ? double(dm.active_tasks + vinfo.active_tasks.size())/double(dm.num_cpus) : 1.0;
        double u_after = dm.memory_size? double(dm.memory_used + bytes_min)/double(dm.memory_size) : 1.0;
        if (c_after > 0.80 || u_after > 0.85) continue;

        auto sm0 = Machine_GetInfo(src), dm0 = dm;
        auto sm1 = sm0, dm1 = dm0;
        sm1.active_tasks = (unsigned)std::max<int>(0, (int)sm0.active_tasks - (int)vinfo.active_tasks.size());
        sm1.memory_used  = (unsigned)std::max<int>(0, (int)sm0.memory_used  - (int)bytes_min);
        dm1.active_tasks = dm0.active_tasks + (unsigned)vinfo.active_tasks.size();
        dm1.memory_used  = dm0.memory_used  + (unsigned)bytes_min;

        auto steadyW = [](const MachineInfo_t& m)->double
        {
            double cu = m.num_cpus ? double(m.active_tasks)/double(m.num_cpus) : 0.0;
            double mu = m.memory_size? double(m.memory_used)/double(m.memory_size) : 0.0;
            return 80.0 + 120.0*(0.5*(cu+mu)) + (m.s_state != S0 ? 30.0 : 0.0);
        };

        double benefit = (steadyW(sm0)+steadyW(dm0)) - (steadyW(sm1)+steadyW(dm1));
        double t_s = double(bytes_min) / 1000.0;
        double cost = 180.0 * t_s;

        if (benefit - cost >= 40.0) 
        {
            VM_Migrate(vm_pick, dst);
            migrating = true;
            break;
        }
    }

    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete", 4);
}

void Scheduler::ChangeComplete(Time_t /*time*/, MachineId_t machine_id) {
    auto it = pending.find(machine_id);
    if (it == pending.end()) return;
    auto tasks = it->second;
    pending.erase(it);

    MachineInfo_t m = Machine_GetInfo(machine_id);
    for (auto tid : tasks) {
        TaskInfo_t ti = GetTaskInfo(tid);

        // try reuse a same-type VM on this machine
        bool placed = false;
        for (auto vm : vms) {                    // now this is legal
            VMInfo_t vi = VM_GetInfo(vm);
            if (vi.machine_id == machine_id && vi.vm_type == ti.required_vm) {
                VM_AddTask(vm, tid, ti.priority);
                placed = true;
                break;
            }
        }
        if (!placed) {
            VMId_t vm = VM_Create(ti.required_vm, ti.required_cpu);
            VM_Attach(vm, machine_id);
            vms.push_back(vm);                   // also legal
            VM_AddTask(vm, tid, ti.priority);
        }
    }
}

void Scheduler::HandleMemoryWarning(Time_t now, MachineId_t mid) {
    // Debounce: ignore if we handled this very recently (e.g., within 1 ms of sim time)
    auto it = last_mem_warn.find(mid);
    if (it != last_mem_warn.end() && (now - it->second) < 1000) return;
    last_mem_warn[mid] = now;

    MachineInfo_t m = Machine_GetInfo(mid);
    if (m.s_state != S0) return; // if it’s asleep or transitioning, nothing to do yet

    // Quarantine this machine from new placements until relieved by PeriodicCheck
    quarantined.insert(mid);

    // Pick a smallest-footprint VM on this machine to migrate
    VMId_t candidate = VMId_t(-1);
    size_t bytes_min = SIZE_MAX;

    for (auto vm : vms) {
        VMInfo_t vi = VM_GetInfo(vm);
        if (vi.machine_id != mid || vi.active_tasks.empty()) continue;

        size_t mem = 0;
        for (auto t : vi.active_tasks) mem += GetTaskInfo(t).required_memory;
        if (mem == 0) mem = 100; // small constant so we can still move tiny VMs
        if (mem < bytes_min) { bytes_min = mem; candidate = vm; }
    }

    if (candidate == VMId_t(-1)) return; // nothing to move

    VMInfo_t cinfo = VM_GetInfo(candidate);

    auto fits_after = [&](MachineId_t dst)->bool {
        auto d = Machine_GetInfo(dst);
        if (d.s_state != S0) return false;
        if (d.cpu != cinfo.cpu) return false;
        double c_after = d.num_cpus ? double(d.active_tasks + cinfo.active_tasks.size())/double(d.num_cpus) : 1.0;
        double u_after = d.memory_size ? double(d.memory_used + bytes_min)/double(d.memory_size) : 1.0;
        return c_after <= 0.80 && u_after <= 0.85;
    };

    // 1) try an existing awake compatible host
    for (auto dst : machines) {
        if (dst == mid) continue;
        if (fits_after(dst)) {
            VM_Migrate(candidate, dst);
            return;
        }
    }

    // 2) if none, wake a compatible sleeper then migrate
    for (unsigned i = 0; i < Machine_GetTotal(); ++i) {
        MachineId_t id = MachineId_t(i);
        if (id == mid) continue;
        auto d = Machine_GetInfo(id);
        if (d.s_state == S0) continue;
        if (d.cpu != cinfo.cpu) continue;
        Machine_SetState(id, S0);
        // migration will complete once S0 is reached; simulator will call MigrationDone
        VM_Migrate(candidate, id);
        return;
    }

    // If we reach here, we’re boxed in. Quarantine still prevents new placements; PeriodicCheck may ease P-states.
}

// Public interface below

static Scheduler Scheduler;

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
    Scheduler.HandleMemoryWarning(time, machine_id);
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
    /*static unsigned counts = 0;
    counts++;
    if(counts == 10) {
        migrating = true;
        VM_Migrate(1, 9);
    }*/
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
    Scheduler.ChangeComplete(time, machine_id);
}