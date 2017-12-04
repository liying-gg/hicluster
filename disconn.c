#include <stdio.h>
#include <unistd.h>
#include "hiredis.h"
#include "sds.h"
#include "hicluster.h"

int main()
{
    redisContext* ctx=redisConnect("10.1.12.15",6379);
    printf("%the err is %d\n",ctx->err);

    sleep(10);

    redisReply *rpy = redisCommand(ctx,"info");
    printf("%the err is %d\n",ctx->err);
    printf("%the errstr is %s\n",ctx->errstr);
    printf("%the err is %d\n",rpy->type);
    if (rpy !=NULL)
    {
        sds out=cliFormatReplyRaw(rpy);
        printf("%s\n",out);
    }

}
