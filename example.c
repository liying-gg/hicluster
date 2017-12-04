#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "hiredis.h"
#include "sds.h"
#include "hicluster.h"

static char *command;

static int parseOptions(int argc, char **argv) {
    int i;

    for (i = 1; i < argc; i++) {
        int lastarg = i==argc-1;

        if (!strcmp(argv[i],"-c") && !lastarg) {
            command = sdsnew(argv[++i]);
        } else {
            if (argv[i][0] == '-') {
                fprintf(stderr,
                    "Unrecognized option or bad number of args for: '%s'\n",
                    argv[i]);
                exit(1);
            } else {
                /* Likely the command name, stop here. */
                break;
            }
        }
    }
    return i;
}

int main(int argc,char * argv[])
{
    char * nodes[5]={0};
    //parseOptions(argc,argv);
    char *node1=sdsnew("127.0.0.1:7000");
    char *node2=sdsnew("127.0.0.1:7001");
    char cmd[64];
//  memset(cmd,0,64);
//  sprintf(cmd,"%s",command);
//  memcpy(cmd,command,strlen(command));
    nodes[0] = node1;
    nodes[1] = node2;
    clusterCtx * ctx = init_connection(nodes,2,5);
    redisReply *rpy = send_cluster_command(ctx,argc-1,(const char **)argv+1,0);
    if (rpy !=NULL)
    {
        sds out=cliFormatReplyRaw(rpy);
        printf("%s\n",out);
    }
    sdsfree(node1);
    sdsfree(node2);
}

