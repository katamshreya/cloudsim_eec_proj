//
//  Scheduler.hpp
//  CloudSim
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <queue>
#include <unordered_map>

#include "Interfaces.h"

struct MachineKey {
    MachineState_t state;
    CPUType_t      cpu;
    bool           gpu;

    bool operator==(const MachineKey& o) const {
        return state == o.state && cpu == o.cpu && gpu == o.gpu;
    }
};
struct MachineKeyHash {
    size_t operator()(const MachineKey& k) const noexcept {
        // simple mixing of small enums + bool
        return (static_cast<size_t>(k.state) * 97u) ^
               (static_cast<size_t>(k.cpu)   * 31u) ^
               (k.gpu ? 0x9e3779b97f4a7c15ULL : 0ULL);
    }
};

struct VirtualKey {
    VMType_t   vm;
    SLAType_t  sla;
    CPUType_t  cpu;
    bool       gpu;

    bool operator==(const VirtualKey& o) const {
        return vm == o.vm && sla == o.sla && cpu == o.cpu && gpu == o.gpu;
    }
};
struct VirtualKeyHash {
    size_t operator()(const VirtualKey& k) const noexcept {
        // mix 4 tiny fields; good enough for small buckets
        size_t h = static_cast<size_t>(k.vm);
        h = h * 131u ^ static_cast<size_t>(k.sla);
        h = h * 131u ^ static_cast<size_t>(k.cpu);
        h = h * 131u ^ static_cast<size_t>(k.gpu);
        return h;
    }
};

class Scheduler {
public:
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);

private:
    // buckets
    std::unordered_map<MachineKey, std::vector<MachineId_t>, MachineKeyHash> hostBuckets;
    std::unordered_map<VirtualKey, std::vector<VMId_t>,     VirtualKeyHash>  vmBuckets;
    std::unordered_map<VirtualKey, std::queue<TaskId_t>,    VirtualKeyHash>  queues;

    // flat tracking (helpful for scans / shutdown)
    std::vector<MachineId_t> allMachines;

    // helpers
    static VMType_t defaultVM(CPUType_t cpu);
    static Priority_t prioFromSLA(SLAType_t sla);
    static bool canHostVM(const MachineInfo_t& mi, VMType_t vmType, CPUType_t vmCpu);
    static bool fitsTask(const MachineInfo_t& mi, const TaskInfo_t& ti);
    void drainQueue(const VirtualKey& key);
};

#endif /* Scheduler_hpp */
