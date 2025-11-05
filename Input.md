machine class:
{
        # general purpose CPU only servers for light tasks, moderate cores/memory, no GPUs.
        Number of machines: 30
        CPU type: X86
        Number of cores: 8
        Memory: 16384
        S-States: [100, 80, 60, 40, 20, 0]
        P-States: [10, 8, 6, 4]
        C-States: [10, 5, 1, 0]
        MIPS: [1000, 800, 600, 400]
        GPUs: no
}
machine class:
{
        # balanced GPU-enabled machines for compute heavy tasks/streaming
        Number of machines: 20
        CPU type: ARM
        Number of cores: 16
        Memory: 32768
        S-States: [150, 120, 100, 80, 50, 10, 0]
        P-States: [16, 12, 8, 4]
        C-States: [15, 10, 5, 0]
        MIPS: [2000, 1500, 1000, 500]
        GPUs: yes
}
machine class:
{
        # servers for long-running/high-performance GPU workloads
        Number of machines: 4
        CPU type: POWER
        Number of cores: 128
        Memory: 131072
        S-States: [250, 200, 150, 100, 50, 10, 0]
        P-States: [64, 32, 16, 8]
        C-States: [50, 25, 10, 0]
        MIPS: [8000, 6000, 4000, 2000]
        GPUs: yes
}
task class:
{
        # light, short lived web requests
        Start time: 0
        End time : 1000000
        Inter arrival: 200
        Expected runtime: 5000
        Memory: 2048
        VM type: LINUX
        GPU enabled: no
        SLA type: SLA0
        CPU type: X86
        Task type: WEB
        Seed: 1001
}
task class:
{
        # compute intensive GPU-enabled tasks (longer runtime)
        Start time: 0
        End time : 1000000
        Inter arrival: 500
        Expected runtime: 20000
        Memory: 8192
        VM type: LINUX
        GPU enabled: yes
        SLA type: SLA1
        CPU type: ARM
        Task type: HPC
        Seed: 1002
}
task class:
{
        # medium duration streaming
        Start time: 0
        End time : 1000000
        Inter arrival: 1000
        Expected runtime: 10000
        Memory: 4096
        VM type: LINUX
        GPU enabled: yes
        SLA type: SLA2
        CPU type: POWER
        Task type: STREAM
        Seed: 1003
}

