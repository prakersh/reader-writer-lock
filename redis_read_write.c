#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>



#define A_START 97
#define A_END 122


//This program required common nfs share mounted accross nodes at /mnt.
char filepath[]="/mnt/tempfile";
//char filepath[]="tempfile";

pthread_t tid_task;
int node_num;

// Set value function to set a key to int in redis KV store.
int set_val(redisContext *c, char *s, int value) {
    int ret;
    redisReply *reply;
    //printf("SET %s %d\n", s, value);
    reply = redisCommand(c, "SET %s %d", s, value);
    ret = reply->integer;
    //printf("RET VAL= %s\n", reply->str);
    freeReplyObject(reply);
    return ret;
}

// Set value function to set a key int to int in redis KV store.
int set_val2(redisContext *c, int s, int value) {
    int ret;
    redisReply *reply;
    //printf("SET %d %d\n", s, value);
    reply = redisCommand(c, "SET %d %d", s, value);
    /*
    if (reply->type == REDIS_REPLY_ERROR) {
           printf("============REL============\n");
        }
    if (reply->type == REDIS_REPLY_NIL) {
           printf("============NIL============\n");
        }
    */
    ret = reply->integer;
    //printf("RET VAL= %s\n", reply->str);
    freeReplyObject(reply);
    return ret;
}

// Get value function to get a key to int from redis KV store.
int get_val(redisContext *c, char *s) {
    int ret;
    redisReply *reply;
    reply = redisCommand(c, "GET %s", s);
    //printf("GET %s %s\n", s, reply->str);
    if (reply->len > 0)
        ret = atoi(reply->str);
    else
        ret=NULL;
    //printf("RET VAL= %s\n", reply->str);
    freeReplyObject(reply);
    return ret;
}
// Get value function to get a key to int from int redis KV store.
int get_val2(redisContext *c, int s) {
    int ret;
    redisReply *reply;
    reply = redisCommand(c, "GET %d", s);
    if (reply->len > 0)
        ret = atoi(reply->str);
    else
        ret=NULL;
    //printf("RET VAL= %s\n", reply->str);
    freeReplyObject(reply);
    return ret;
}

// Increment value of key s by 1
int inc_val(redisContext *c, char *s) {
    int ret;
    ret = get_val(c, s);
    ret++;
    set_val (c, s, ret);
    return ret;
}

// Decrement value of key s by 1
int dec_val(redisContext *c, char *s) {
    int ret;
    ret = get_val(c, s);
    ret--;
    set_val (c, s, ret);
    return ret;
}

// Del value function to del a key from redis KV store.
int del_val(redisContext *c, char *s) {
    int ret;
    redisReply *reply;
    //printf("DEL %s\n", s);
    reply = redisCommand(c, "DEL %s", s);
    if (reply->len > 0)
        ret = atoi(reply->str);
    else
        ret = reply->integer;
    //printf("RET VAL= %s\n", reply->str);
    freeReplyObject(reply);
    return ret;
}

// Get value function to get a key to int from redis KV store.
char* get_str(redisContext *c, char *s) {
    int ret;
    redisReply *reply;
    reply = redisCommand(c, "GET %s", s);
    if (reply->len > 0)
        ret = reply->str;
    else
        ret=NULL;
    //printf("RET VAL= %s\n", reply->str);
    freeReplyObject(reply);
    return ret;
}

// Initailaise spin lock.
int init_spnlock(redisContext *c,  char *s ) {
    return set_val(c, s, 0);
}

// Initailaise spin lock.
int init_spnlock2(redisContext *c,  int s ) {
    return set_val2(c, s, 0);
}

//Spinlock lock function.
int spn_lock(redisContext *c,  char *s ) {
    int ret = 1;
    redisReply *reply;
    while (ret != 0) {
        reply = redisCommand(c, "GETSET %s 1", s);
        if (reply->len > 0) {
            if (strcmp(reply->str, "0") == 0){
                //printf("RET VAL= %s\n", reply->str);
                ret = 0;
            }
        }
        freeReplyObject(reply);
    }
    return ret;
}

// Spinlock lock2 function.
int spn_lock2(redisContext *c,  int s ) {
    int ret = 1;
    redisReply *reply;
    //printf("Getting lock %d\n", s);
    //printf("LOCK VAL = %d\n", get_val2(c,s));
    while (ret != 0) {
        reply = redisCommand(c, "GETSET %d 1", s);
        if (reply == NULL) {
           printf("NULL VAL");
        }
        if (reply->type == REDIS_REPLY_ERROR) {
           printf("ERROR VAL");
        }
        //printf("M:  %d : %s",reply->integer, reply->str);

        if (reply->type == REDIS_REPLY_STRING) {
            if (strcmp(reply->str, "0") == 0){
                //printf("RET VAL= %s\n", reply->str);
                ret = 0;
            }
        }
        else {
            if (reply->type == REDIS_REPLY_NIL)
               printf("RET NIL\n");
        }
        freeReplyObject(reply);
    }
    //printf("Got lock %d\n", s);
    return ret;
}
//Spinlock unlock function.
int spn_unlock( redisContext *c, char *s ) {
    //printf("Unlocking lock %s\n", s);
    return set_val(c, s, 0);
}


//Spinlock unlock2 function.
int spn_unlock2( redisContext *c, int s ) {
    //printf("Unlocking lock %d\n", s);
    return set_val2(c, s, 0);
    //printf("Getting lock %d\n", get_val2(c,s));
    //return 0;
}

// Function to create queue.
int createQueue(redisContext *c) {
    int tmp[100];
    int out;
    out = rand();
    while (get_val2(c, out) != NULL )
    {
        //printf("SKIP = %s\n", out);
        out = rand();
        //printf("NEW STR %s\n", out);
    }
    //printf("GOT QueueID = %d\n", out);
    sprintf(tmp, "%d_NUM", out);
    set_val(c, tmp, 0);
    set_val2(c, out, 0);
    return out;
}

// Function to create a new node.
int newNode(redisContext *c, int k)  {
    int out;
    char tmp[100];
    out = rand();
    while (get_val2(c, tmp) != NULL )
    {
        //printf("SKIP = %d\n", out);
        out = rand();
        //printf("NEW STR %d\n", out);
    }
    //printf("GOT NodeID = %d\n", out);
    set_val2(c, out, 1);

    sprintf(tmp, "%d_KEY", out);
    set_val( c, tmp, k);
    return out;
}

// The function to add key k to q.
void enQueue(redisContext *c, int q, int k)  {
    // create a new linked list node
    int temp = newNode(c, k);
    char tmp[100];

    // if queue is empty, then new node is front and rear both
    sprintf(tmp, "%d_REAR", q);
    if (get_val(c, tmp) == NULL) {
        sprintf(tmp, "%d_FRONT", q);
        set_val( c, tmp, temp);
        sprintf(tmp, "%d_REAR", q);
        set_val( c, tmp, temp);
    }
    else {
        // add the new node at the end of queue and change rear
        sprintf(tmp, "%d_REAR", q);
        //printf("HEEEE\n");
        sprintf (tmp, "%d_NEXT", get_val(c, tmp));
        //printf("%s\n", tmp);
        set_val( c, tmp, temp);
        sprintf(tmp, "%d_REAR", q);
        set_val( c, tmp,  temp);
    }
    return;
}

// Function to remove a key from given queue q.
// This function will also send wake signal to removed node.
int deQueue(redisContext *c, int q) {
    int ret_val, temp2;
    char tmp[100];
    // if queue is empty, return 0.
    sprintf(tmp, "%d_FRONT", q);
    if (get_val(c, tmp) == NULL)
        return NULL;

    // store previous front and move front one node ahead
    sprintf(tmp, "%d_FRONT", q);
    int temp = get_val(c, tmp);

    sprintf(tmp, "%d_FRONT", q);
    sprintf (tmp, "%d_NEXT", get_val(c, tmp));
    temp2 = get_val(c, tmp);
    sprintf(tmp, "%d_FRONT", q);
    set_val( c, tmp, temp2);
    // if front becomes NULL, then change rear also as NULL
    sprintf(tmp, "%d_FRONT", q);
    if (get_val(c, tmp) == NULL) {
        sprintf(tmp, "%d_REAR", q);
        del_val(c, tmp);
    }
    sprintf(tmp, "%d_KEY", temp);
    ret_val = get_val(c, tmp);
    sprintf(tmp, "%d_REAR", temp);
    del_val(c, tmp);
    sprintf(tmp, "%d_NEXT", temp);
    del_val(c, tmp);
    sprintf(tmp, "%d", temp);
    del_val(c, tmp);
    printf("Waking node%d\n", ret_val);
    redisCommand(c, "PUBLISH redis_reader_writer %d", ret_val);
    return ret_val;
}

void signal_handler(int sig) {
        //printf("Caught signal %d\n", sig);
}

// Wait function for reader writer lock.
void wait_rwl(redisContext *c, int rwl, int q) {
    int temp;
    char tmp[100];
    spn_unlock2(c, rwl);
    spn_lock2(c, q);
    // Add self to queue.
    enQueue(c, q, node_num);
    spn_unlock2(c, q);
    printf("Pausing\n");
    pause();
    printf("Awake\n");
    spn_lock2(c, rwl);
    return;
}

// Init reader writer lock.
// To be done by any 1 node once.
// It will return a int.
// RETURN has the key which will be used for spin lock.
// RETURN_NA has value of active nodes on RWL.
// RETURN_RQ has the key for Reader Queue.
// RETURN_WQ has the key for Writer Queue.
int init_rwlock (redisContext *c) {
    int out, temp;
    char tmp[100];
    out = rand();
    while (get_val2(c, out) != NULL )
    {
        //printf("SKIP = %s\n", out);
        out = rand();
        //printf("NEW STR %s\n", out);
    }
    init_spnlock2(c, out);
    printf("RWL: %d\n", out);

    temp = createQueue(c);
    sprintf(tmp, "%d_RQ", out);
    set_val( c, tmp, temp);
    printf("RQ: %d\n", temp);

    temp = createQueue(c);
    sprintf(tmp, "%d_WQ", out);
    set_val( c, tmp, temp);
    sprintf(tmp, "%d_NA", out);
    set_val(c, tmp, 0);
    printf("WQ: %d\n", temp);
    return out;
}

// Get Shared Lock.
void lockShared (redisContext *c, int rwl) {
    int WQ, RQ, NA, PendingWrites;
    char tmp[100];

    // Getting reader writer lock.
    spn_lock2 (c, rwl);

    sprintf(tmp, "%d_WQ", rwl);
    WQ = get_val(c, tmp);
    sprintf(tmp, "%d_RQ", rwl);
    RQ = get_val(c, tmp);

    //Increasing pending reader count by 1.
    spn_lock2(c, RQ);
    sprintf(tmp, "%d_NUM", RQ);
    inc_val(c, tmp);
    spn_unlock2(c, RQ);

    spn_lock2(c, WQ);
    sprintf(tmp, "%d_NUM",WQ);
    PendingWrites = get_val(c, tmp);
    if (PendingWrites > 0) {
        // Don't starve writers
        spn_unlock2(c, WQ);
        wait_rwl(c, rwl, RQ);
    }
    else
        spn_unlock2(c, WQ);
    sprintf(tmp, "%d_NA", rwl);
    NA = get_val(c, tmp);
    // Someone has exclusive lock
    while (NA < 0) {
        wait_rwl(c, rwl, RQ);
        NA = get_val(c, tmp);
    }
    // Increase NA by 1.
    inc_val(c, tmp);
    spn_unlock2 (c, rwl);

    //Decreasing pending reader count by 1.
    sprintf(tmp, "%d_NUM", RQ);
    spn_lock2(c, RQ);
    dec_val(c, tmp);
    spn_unlock2(c, RQ);
}

// Unlock shared lock.
void unlockShared (redisContext *c,  int rwl) {
    int WQ, RQ, NA;
    char tmp[100];

    sprintf(tmp, "%d_WQ", rwl);
    WQ = get_val(c, tmp);
    sprintf(tmp, "%d_RQ", rwl);
    RQ = get_val(c, tmp);

    spn_lock2 (c, rwl);
    sprintf(tmp, "%d_NA", rwl);
    dec_val(c, tmp);
    // If NA is 0 signal pending writer if any.
    NA = get_val(c, tmp);
    if (NA == 0) {
        spn_unlock2 (c, rwl);
        deQueue(c, WQ);
        }
    else
        spn_unlock2 (c, rwl);
 }

// Get exclusive lock.
void lockExclusive (redisContext *c, int rwl) {
    int WQ, RQ, NA, temp;
    char tmp[100];

    sprintf(tmp, "%d_WQ", rwl);
    WQ = get_val(c, tmp);
    sprintf(tmp, "%d_RQ", rwl);
    RQ = get_val(c, tmp);

    //Increasing pending writer count by 1.
    spn_lock2(c, WQ);
    sprintf(tmp, "%d_NUM", WQ);
    inc_val(c, tmp);
    spn_unlock2(c, WQ);

    sprintf(tmp, "%d_NA", rwl);
    spn_lock2 (c, rwl);
    NA = get_val(c, tmp);
    while (NA) {
        wait_rwl(c, rwl, WQ);
        NA = get_val(c, tmp);
    }
    //set_val2(c, tmp, "-1");
    redisCommand(c, "SET %s -1", tmp);

    sprintf(tmp, "%d_NUM", WQ);
    //Increasing pending writer count by 1.
    spn_lock2(c, WQ);
    dec_val(c, tmp);
    spn_unlock2(c, WQ);

    spn_unlock2 (c, rwl);
}

//
void unlockExclusive (redisContext *c, int rwl) {

    int WQ, RQ, NA, PendingReads;
    char tmp[100];

    sprintf(tmp, "%d_WQ", rwl);
    WQ = get_val(c, tmp);
    sprintf(tmp, "%d_RQ", rwl);
    RQ = get_val(c, tmp);

    spn_lock2 (c, rwl);
    sprintf(tmp, "%d_NA", rwl);
    set_val(c, tmp, 0);
    spn_unlock2 (c, rwl);

    spn_lock2(c, RQ);
    sprintf(tmp, "%d_NUM",RQ);
    PendingReads = get_val(c, tmp);
    if (PendingReads != 0) {
        spn_unlock2 (c, RQ);
        // Wake all readers.
        while (deQueue(c, RQ) != NULL)
            ;;
    }
    else {
        spn_unlock2 (c, RQ);
        // Wake a writer.
        deQueue(c, WQ);
    }
 }


void *task() {
    char buffer[100];
    char rwl_var_str[]="rwl_var";
    int ref_var=1;
    int rwl;

    redisReply *reply;
    redisContext *c = redisConnect("10.3.71.88", 6379);
    if (c != NULL && c->err)
        printf("Error: %s\n", c->errstr);
    else
        printf("Connected to Redis\n");


    // Authenticating to redis server
    reply = redisCommand(c, "AUTH ssl12345");
    freeReplyObject(reply);
    signal(SIGUSR1, signal_handler);

    // Doing initalisation on node0.
    if (node_num == 0) {
        // Pause mechanism to prevent node 1 and 2 to start
        // before node 0 initalises lock and ref_var.
        set_val (c, "start_tc", 0);
        // Initalising Spin Lock
        rwl = init_rwlock(c);
        // Removing older Filename
        remove(filepath);
        FILE *out = fopen(filepath, "a");
        fprintf(out, "DUMMY_INIT");
        fclose(out);
        set_val (c, rwl_var_str, rwl);
        set_val (c, "start_tc", 1);
    }
    // Wait for initalisation on node0.
    else{
        set_val (c, "start_tc", 0);
        while (get_val (c, "start_tc") != 1)
            continue;
        rwl = get_val (c, rwl_var_str);
    }
    while (ref_var <= 10 ){
        printf("Running iteration: %d on node %d....\n", ref_var, node_num);
        // node2 and node 3 do write and sleeps for 5 seconds.
        if (node_num >=2) {
            //printf("Getting rwl\n");
            lockExclusive (c, rwl);
            printf("Writing on node %d\n", node_num);
            FILE *out = fopen(filepath, "w");
            fprintf(out, "WRITE_ITERATION_VAL: %d written by node%d", ref_var, node_num);
            fclose(out);
            printf("node%d sleeping before unlock.\n", node_num);
            sleep(5);
            //printf("Now relasing rwl\n");
            unlockExclusive (c, rwl);
        }
        //node0 does read and then sleeps for 8 seconds.
        //node1 does read and sleeps for 5 seconds.
        if (node_num == 0 || node_num == 1 ) {
            //printf("Getting rwl\n");
            lockShared (c, rwl);
            printf("Reading on node %d\n", node_num);
            FILE *out = fopen(filepath, "r");
            fread(buffer, 100, 1, out);
            printf("OUTPUT_READ: \n%s\n", buffer);
            fclose(out);
            printf("node%d sleeping before unlock.\n", node_num);
            sleep(5);
            //printf("Now relasing rwl\n");
            if (node_num == 0 )
                sleep(3);
            unlockShared (c, rwl);
        }
        //printf("Now relased rwl\n");
        printf("------------------------------\n");
        ref_var++;
    }
    set_val (c, "start_tc", 0);
    printf("Node%d is exiting.\n", node_num);
    redisFree(c);
    return 0;
}

// Function to wake sleeping thead when threadnum is recieved.
void onMessage(redisAsyncContext * c2, void *reply, void * privdata) {
    int ret;
    redisReply * r = reply;
    if (reply == NULL)
        return;
    if (r->type == REDIS_REPLY_ARRAY) {
        if ( r->element[2]->str != NULL ){
            ret = atoi(r->element[2]->str);
            if  ( ret == node_num) {
                printf("Sending signal to wake paused thread\n");
                pthread_kill(tid_task, SIGUSR1);
            }
        }
    }
}

// Function to listen to pubsub channel.
void *wake_task() {
    signal(SIGPIPE, SIG_IGN);
    struct event_base * base = event_base_new();
    redisAsyncContext * c2 = redisAsyncConnect("10.3.71.88", 6379);
    if (c2->err) {
        printf("error: %s\n", c2->errstr);
        return 1;
    }
    redisReply *reply;
    reply = redisCommand(c2, "AUTH ssl12345");
    freeReplyObject(reply);

    redisLibeventAttach(c2, base);
    redisAsyncCommand(c2, onMessage, NULL, "SUBSCRIBE redis_reader_writer");
    event_base_dispatch(base);

    redisFree(c2);
    return 0;
}
int main(int argc, char **argv) {
    pthread_t thread2;
	if (argc != 2){
	printf("HELP: USAGE ./a.out n (n is node number can be 0, 1, or 3) \n");
	return 1;
	}
	node_num = atoi(argv[1]);
    if ( (node_num < 0) || (node_num > 3)) {
        printf("Node Number can only be 0, 1, or 3.\n");
        return 1;
    }
    else
        printf("This is Node number %d\n", node_num);
    srand(time(0));
    pthread_create(&tid_task, NULL, task, NULL);
    pthread_create(&thread2, NULL, wake_task, NULL);
    //task(c, node_num);
    pthread_join(tid_task, NULL);
    return 0;
}
