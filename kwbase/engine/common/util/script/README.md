## About
Error injection instructions

## How to use
1、cmake compiles with -DK_DO_NOT_SHIP=OFF
for example：
```bash
> mkdir build && cd build
> cmake .. -DCMAKE_BUILD_TYPE=Debug -DWITH_DEFINITION=K_DEBUG -DWITH_TESTS=OFF -DBUILD_KWBASE=ON -DWITH_OPENSSL=ON -DK_DO_NOT_SHIP=OFF
```

2、Before starting each kwbase, a separate KW_ERR_INJECT_PORT needs to be set
for example：
```bash
> cd install/bin
> export KW_ERR_INJECT_PORT=23333
> export LD_LIBRARY_PATH=../lib
> ./kwbase start --insecure --listen-addr=127.0.0.1:26257 --http-addr=127.0.0.1:8081 --store=./kwbase-data1 --background
```

3、Error injection
Help:
```
./err_inject.sh port1,port2 trigger fault_id when [val]
./err_inject.sh port1 active fault_id when [val]
./err_inject.sh port1,port2 deactive fault_id
./err_inject.sh port1 ls
```
Instructions：  
(1) port: KW_ERR_INJECT_PORT corresponding to when the kwbase process starts
    trigger/active/ls: Required, command keyword, cannot be modified
    fault_id: Required, corresponding to the fault displayed using the ls command
    when: Required, triggered, or effective time (how many calls are triggered or effective)
    val: Optional, exception val value to be injected, use caution (data type mismatches can cause downtime)
(2) trigger: Trigger command. This command will trigger an error injection of fault_id (optional error injection val) on the first call. Subsequent calls will not trigger this error injection
(3) active: Execute command, which will make the fault_id error injection take effect the first time it is called (optional val value for error injection)
(4) deactive: This command invalidates the fault_id error injection
(5) ls: View the command. This command will display information about all faults injected into fault_id. Note: Before using error injection, you need to run the ls command to load fault_id
for example：
```bash
> cd install/bin
-- view all fault
> bash ./err_inject.sh 23333 ls
No.     Id                                       actived history when    cur     val     
0       FAULT_CM_CONFIG_FILE_FAIL                0       0       0       0               
1       FAULT_CONTEXT_INIT_FAIL                  0       0       0       0               
2       FAULT_DS_HASH_MAP_MALLOC_FAIL            0       0       0       0               
3       FAULT_EE_EXECUTOR_APPLY_THREAD_MSG_FAIL  0       0       0       0               
4       FAULT_EE_DML_SETUP_PREINIT_MSG_FAIL      0       0       0       0                   
Success
-- FAULT_CONTEXT_INIT_FAIL injection
> bash ./err_inject.sh 23333 trigger FAULT_CONTEXT_INIT_FAIL 2
Success
-- Cancel FAULT_CONTEXT_INIT_FAIL injection
> bash ./err_inject.sh 23333 deactive FAULT_CONTEXT_INIT_FAIL
Success
-- FAULT_CONTEXT_INIT_FAIL injection takes effect
> bash ./err_inject.sh 23333 active FAULT_CONTEXT_INIT_FAIL 10
Success
```