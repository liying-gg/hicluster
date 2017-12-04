/* HiclusterLib, A C library for redis cluster access
 *
 * Copyright (c) , Liying Chen<liying_gg at hotmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include "hiredis.h"
#include "sds.h"
#include "dict.c"
#include "hicluster.h"

typedef unsigned long long cycles_t;
static inline cycles_t currentcycles() {
    cycles_t result;
    __asm__ __volatile__ ("rdtsc" : "=A" (result));
    return result;
}

static unsigned int slotHash(const void *key) {
    int * hash = (int*)key;
    return *hash;
}

static int slotKeyCompare(void *privdata, const void *key1, const void *key2) {
    int l1, l2;
    ((void) privdata);

    l1 = *(int*)key1;
    l2 = *(int*)key2;
    if (l1 != l2) return 0;
    return 1;
}

static void *slotKeyDup(void *privdata, const void *src) {
    ((void) privdata);
    int *dup = malloc(sizeof(*dup));
    memcpy(dup,src,sizeof(*dup));
    return dup;
}

static void *slotValDup(void *privdata, const void *src) {
    ((void) privdata);
    slot *dup = malloc(sizeof(*dup));
    dup->count=((slot*)src)->count;
    int count= dup->count;
    int i;

    for (i=0;i<count;i++)
        dup->nodes[i]=sdsnew(((slot*)src)->nodes[i]);
    
    return dup;
}

static void slotKeyDestructor(void *privdata, void *key) {
    ((void) privdata);
    free(key);
}

static void slotValDestructor(void *privdata, void *val) {
    ((void) privdata);
    slot *sl = (slot*)val;
    int count = sl->count;
    while (count >0)
    {
        sdsfree(sl->nodes[count-1]);
        count--;
    }
    free(val);
}

static unsigned int strHash(const void *key) {
    int len = strlen((const unsigned char *)key);
    const unsigned char * buf = (const unsigned char *)key;

    unsigned int hash = 5381;

    while (len--)
        hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */
    return hash;
}

static void *strKeyDup(void *privdata, const void *src) {
    ((void) privdata);
    char *dup = malloc(strlen(src));
    memcpy(dup,src,strlen(src));
    return dup;
}

static int strKeyCompare(void *privdata, const void *key1, const void *key2) {
    int l1, l2;
    ((void) privdata);

    if (key1==NULL || key2==NULL) return 0;

    l1 = strlen((const char *)key1);
    l2 = strlen((const char *)key2);
    if (l1 != l2) return 0;
    return memcmp(key1,key2,l1) == 0;
}

static void strKeyDestructor(void *privdata, void *key) {
    ((void) privdata);
    free(key);
}

static void strValDestructor(void *privdata, void *val) {
    ((void) privdata);
    redisFree((redisContext *)val);
}

static dictType slotDict = {
    slotHash,
    slotKeyDup,
    slotValDup,
    slotKeyCompare,
    slotKeyDestructor,
    slotValDestructor
};

static dictType connDict = {
    strHash,
    strKeyDup,
    NULL,
    strKeyCompare,
    strKeyDestructor,
    strValDestructor
};

/* Get the redis connection*/
static redisContext * get_redis_link(char * ip,int port)
{
    redisContext *c = redisConnect(ip, port);
    if (c == NULL || c->err)
    {
        if (c) {
            printf("Error: %s\n", c->errstr);
        } else {
            printf("Can't allocate redis context\n");
        }
        return NULL;
    }
    else
    {
        return c;
    }
}

/* Format the output to a type of strings */
sds cliFormatReplyRaw(redisReply *r) {
    sds out = sdsempty(), tmp;
    size_t i;

    switch (r->type) {
    case REDIS_REPLY_NIL:
        /* Nothing... */
        break;
    case REDIS_REPLY_ERROR:
        out = sdscatlen(out,r->str,r->len);
        out = sdscatlen(out,"\n",1);
        break;
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
        out = sdscatlen(out,r->str,r->len);
        break;
    case REDIS_REPLY_INTEGER:
        out = sdscatprintf(out,"%lld",r->integer);
        break;
    case REDIS_REPLY_ARRAY:
        for (i = 0; i < r->elements; i++) {
            if (i > 0) out = sdscat(out,"\n");
            tmp = cliFormatReplyRaw(r->element[i]);
            out = sdscatlen(out,tmp,sdslen(tmp));
            sdsfree(tmp);
        }
        break;
    default:
        fprintf(stderr,"Unknown reply type: %d\n", r->type);
        exit(1);
    }
    return out;
}

/* Get the cluster node map, Initialize the slots hash table*/
static void initialize_slots_cache(clusterCtx * ctx)
{
    int i,j,k;
    char address[64]={0};
    char * ip;
    char * port;

    /* Recreate the slot cache */
    flush_slots_cache(ctx);
    ctx->allSlot=dictCreate(&slotDict,NULL);

    for (i=0;i<ctx->nodes_num;i++)
    {
        memcpy(address,ctx->startup_nodes[i],strlen(ctx->startup_nodes[i]));

        ip = strtok(address,":");
        port = strtok(NULL,":");
        redisContext * c = get_redis_link(ip,atoi(port));
        if (c==NULL || c->err != 0 )
            continue;

        redisReply * reply = redisCommand(c, "cluster slots");

        if (reply==NULL || reply->type == REDIS_REPLY_ERROR) 
            continue;
        else
        {
            for (j=0;j<reply->elements;j++)
            {
                sds out=cliFormatReplyRaw(reply->element[j]);

                char * subtoken;
                int start = atoi(strtok(out, "\n"));
                int end = atoi(strtok(NULL, "\n"));
                slot * sl = (slot *)malloc(sizeof(slot));
                memset(sl,0,sizeof(slot));

                while(sl->count <= SIZE)
                {
                    sds node=sdsempty();
                    subtoken = strtok(NULL, "\n");
                    if (subtoken == NULL)
                    {
                        break;
                    }
                    node = sdscatlen(node,subtoken,strlen(subtoken));
                    node = sdscat(node,":");
                    subtoken = strtok(NULL, "\n");
                    if (subtoken == NULL)
                    {
                        break;
                    }
                    node = sdscatlen(node,subtoken,strlen(subtoken));
                    sl->nodes[sl->count] = node;
                    sl->count++;
                }

                for (k=start;k<=end;k++)
                {
                    dictAdd(ctx->allSlot,&k,sl);
                }

                /* Free the local memory because we already add the sl to hash table which will allocate memory for the stuff */
                while (sl->count >0)
                {
                    sdsfree(sl->nodes[sl->count-1]);
                    sl->count--;
                }
                free(sl);

                sdsfree(out);
            }

            freeReplyObject(reply);
            redisFree(c);
            break;
        }
    }
    ctx->refresh_tab_asap=0;
}

/* Delete the cached slot hash table */
static void flush_slots_cache(clusterCtx * ctx)
{
    if (ctx->allSlot != NULL)
        dictRelease(ctx->allSlot);
}

/* Compute the hash value for the key */
static unsigned int keyslot(char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    /* Search the first occurrence of '{'. */
    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key,keylen) & 16383;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16(key,keylen) & 16383;

    /* If we are here there is both a { and a } on its right. Hash
 *      * what is in the middle between { and }. */
    return crc16(key+s+1,e-s-1) & 16383;
}

/* Get the key string from the command */
static char * get_key_from_command(const char ** argv)
{
    char * key = (char *)argv[0];
    if ((strcmp(key,"info") == 0)  || (strcmp(key,"multi") == 0) || (strcmp(key,"exec") == 0) || (strcmp(key,"slaveof") == 0) || (strcmp(key,"config") == 0) || (strcmp(key,"shutdown") == 0) )
        return NULL;
    else
        return (char *)argv[1];
}

static void close_existing_connection(clusterCtx *ctx)
{
    if (ctx->allConn!=NULL && ctx->allConn->used > MAX_CONN)
    {
        int r;
        char * key=NULL;
        srand((unsigned int)currentcycles());
        r=rand()%(ctx->allConn->used+1);

        dictIterator *it = dictGetIterator(ctx->allConn);
        dictEntry *de;
        while ((de = dictNext(it)) != NULL && r-->0)
        {
            key = (char *)de->key;
        }

        dictDelete(ctx->allConn,key);
    }
}

/* Get the random connection */
static redisContext * get_random_connection(clusterCtx * ctx)
{
    char address[64];
    slot *sl;
    int r=0;

    if (ctx->allSlot != NULL)
    {
        /*This should be modify because the time will don't change for one second*/
        srand((unsigned int)currentcycles());
        r=rand()%(ctx->allSlot->used+1);
        dictIterator *it = dictGetIterator(ctx->allSlot);
        dictEntry *de;
        while ((de = dictNext(it)) != NULL && r-->0)
        {
            sl = (slot*)de->val;
        }
        if (sl==NULL)
            return NULL;
        memcpy(address,sl->nodes[0],strlen(sl->nodes[0]));
    }
    else
        return NULL;

    dictEntry * entry = dictFind(ctx->allConn, address);
    if (entry == NULL)
    {
        close_existing_connection(ctx);
        char * ip = strtok(address,":");
        char * port = strtok(NULL,":");
    
        redisContext * c = get_redis_link(ip,atoi(port));
        if (c==NULL || c->err != 0 )
            return NULL;
    
        redisReply * reply = redisCommand(c, "PING");

        if(reply==NULL || strcmp(reply->str,"PONG")!=0)
            return NULL;

        dictAdd(ctx->allConn,sl->nodes[0],c);
    
        return c;
    }
    else
    {
        redisReply * reply = redisCommand((redisContext*)entry->val, "PING");

        if(reply==NULL || strcmp(reply->str,"PONG")!=0)
            return NULL;

        return (redisContext *)entry->val;
    }
}

/* Get specified connection by the slot index value */
static redisContext * get_connection_by_slot(clusterCtx * ctx,int index)
{
    dictEntry *re=dictFind(ctx->allSlot,&index);
    /*If we don't know what the mapping is, return a random node.*/
    if (re == NULL)
    {
        return get_random_connection(ctx);
    }

    slot * st = (slot*)(re->val);
    if (st == NULL)
    {
        return get_random_connection(ctx);
    }

    dictEntry * entry = dictFind(ctx->allConn, st->nodes[0]);
    
    if (entry == NULL)
    {
        close_existing_connection(ctx);

        char address[64];
        char * ip;
        char * port;

        memcpy(address,st->nodes[0],strlen(st->nodes[0]));
        ip = strtok(address,":");
        port = strtok(NULL,":");

        redisContext * c = get_redis_link(ip,atoi(port));
        if (c==NULL || c->err != 0 )
            return get_random_connection(ctx);

        redisReply * reply = redisCommand(c, "PING");

        if(reply==NULL || strcmp(reply->str,"PONG")!=0)
            return get_random_connection(ctx);

        dictAdd(ctx->allConn,st->nodes[0],c);

        return c;
    }
    else
    {
        redisReply * reply = redisCommand((redisContext*)entry->val, "PING");

        if(reply==NULL || strcmp(reply->str,"PONG")!=0)
            return get_random_connection(ctx);

        return (redisContext *)entry->val;
    }
}

/* Send the command to the cluster */
redisReply * send_cluster_command(clusterCtx * ctx,int argc,const char **argv, const size_t *argvlen)
{
    int ttl = RedisClusterRequestTTL;
    int asking = 0;
    int try_random_node = 0;
    int index=0,len=0;
    redisContext * conn =NULL;

    if (ctx->refresh_tab_asap == 1)
        initialize_slots_cache(ctx);

    /*Get the index value for the specified key*/
    char * key = get_key_from_command(argv);
    if (key==NULL)
    {
        try_random_node = 1;
    }
    else
        index = keyslot(key,strlen(key));

    while (ttl >0)
    {
        ttl--;
        if (try_random_node==1)
        {
            conn = get_random_connection(ctx);
            try_random_node = 0;
        }
        else
            conn = get_connection_by_slot(ctx,index);
     
        if (conn == NULL)
        {
            try_random_node=1;
            continue;
        }

        if (asking == 1)
        {
            redisReply * ask = redisCommand(conn, "asking");
            asking=0;
            redisReply * rpy = redisCommandArgv(conn,argc,argv,argvlen);
            if (ask==NULL || ask->type==REDIS_REPLY_ERROR || rpy==NULL || rpy->type==REDIS_REPLY_ERROR)
            {
                try_random_node = 1;
                if (ttl < RedisClusterRequestTTL/2)
                    usleep(100);
            }
            else
                return rpy;
        }
        else
        {
            redisReply * reply = redisCommandArgv(conn,argc,argv,argvlen);

            if (reply == NULL || conn->err != 0)
            {
                try_random_node=1;
                continue;
            }

            if (reply->type!=REDIS_REPLY_ERROR)
            {
                return reply;
            }
            else
            {
                sds out=cliFormatReplyRaw(reply);
                if (strncmp(out,"MOVED",5) == 0 || strncmp(out,"ASK",3) == 0 )
                {
                    if (strncmp(out,"ASK",3) == 0 )
                        asking=1;
                    else
                        ctx->refresh_tab_asap=1;
                        
                    int idx;
                    char * address;
                    strtok(out," ");
                    idx = atoi(strtok(NULL," "));
                    address = strtok(NULL," ");
                   
                    slot * sl = (slot *)malloc(sizeof(slot));
                    memset(sl,0,sizeof(slot));
 
                    sds node=sdsempty();
                    node = sdscatlen(node,address,strlen(address));
                    sl->nodes[sl->count] = node;
                    sl->count++;

                    dictDelete(ctx->allSlot,&idx);
                    dictAdd(ctx->allSlot,&idx,sl);

                    /* Free the local allocated memory */
                    while (sl->count >0)
                    {
                        sdsfree(sl->nodes[sl->count-1]);
                        sl->count--;
                    }
                    free(sl);
                    continue;
                }
                else
                    return reply;
            }
        }
    }
    printf("Too many redirection\n");
    return NULL;
}

/* Send the set command to the cluster */
redisReply * method_set(char * cmd)
{
/*
    return send_cluster_command(cmd);
*/
}

/* Send the get command to the cluster */
redisReply * method_get(char * cmd)
{
 //   return send_cluster_command(cmd);
}

/* Init the dict and slot cache */
clusterCtx * init_connection(char * nodes[],int size,int maxconn)
{
    int i;
    clusterCtx *ctx = (clusterCtx *) malloc(sizeof(*ctx));
    ctx->nodes_num = size;
    ctx->startup_nodes = malloc(ctx->nodes_num*sizeof(sds));
    for (i=0;i<ctx->nodes_num;i++)
        ctx->startup_nodes[i]=sdsnew(nodes[i]);
     
    ctx->allConn=dictCreate(&connDict,NULL);
    ctx->refresh_tab_asap=1;
    initialize_slots_cache(ctx);
    return ctx;
}
void destroy_connection(clusterCtx * ctx)
{
    int i;
    for (i=0;i<ctx->nodes_num;i++)
        sdsfree(ctx->startup_nodes[i]);
    free(ctx->startup_nodes);
    flush_slots_cache(ctx);
    if (ctx->allConn!=NULL)
        dictRelease(ctx->allConn);
}
