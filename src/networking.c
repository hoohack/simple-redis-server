/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "server.h"
#include "atomicvar.h"
#include "util.h"
#include "zmalloc.h"
#include <unistd.h>
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>

extern struct redisServer server;
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
void decrRefCountVoid(void *o);

static void setProtocolError(const char *errstr, client *c, int pos);

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
	void *sh = sdsAllocPtr(s);
	return zmalloc_size(sh);
}

/* Return the amount of memory used by the sds string at object->ptr
 * for a string object. */
size_t getStringObjectSdsUsedMemory(robj *o) {
	switch(o->encoding) {
		case OBJ_ENCODING_RAW: return sdsZmallocSize(o->ptr);
		case OBJ_ENCODING_EMBSTR: return zmalloc_size(o)-sizeof(robj);
		default: return 0; /* Just integer encoding for now. */
	}
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
	return sdsdup(o);
}

void freeClientReplyValue(void *o) {
	sdsfree(o);
}

/*
 * 在运行中的服务器创建一个redisClient对象
 * 并注册回调函数，当客户端有数据到来时触发
 */
client *createClient(int fd) {
	client *c = zmalloc(sizeof(client));

	/* passing -1 as fd it is possible to create a non connected client.
	 * This is useful since all the commands needs to be executed
	 * in the context of a client. When commands are executed in other
	 * contexts (for instance a Lua script) we need a non connected client. */
	if (fd != -1) {
		anetNonBlock(NULL,fd);
		anetEnableTcpNoDelay(NULL,fd);
		if (server.tcpkeepalive)
			anetKeepAlive(NULL,fd,server.tcpkeepalive);
		// 注册readQueryFromClient回调函数
		if (aeCreateFileEvent(server.el,fd,AE_READABLE,
					readQueryFromClient, c) == AE_ERR)
		{
			close(fd);
			zfree(c);
			return NULL;
		}
	}

	c->fd = fd;
	c->name = NULL;
	c->bufpos = 0;
	c->querybuf = sdsempty();
	c->querybuf_peak = 0;
	c->reqtype = 0;
	c->argc = 0;
	c->argv = NULL;
	c->cmd = c->lastcmd = NULL;
	c->multibulklen = 0;
	c->bulklen = -1;
	c->reply = listCreate();
	c->reply_bytes = 0;
	listSetFreeMethod(c->reply,freeClientReplyValue);
	listSetDupMethod(c->reply,dupClientReplyValue);
	if (fd != -1) listAddNodeTail(server.clients,c); // 添加成功创建的客户端对象到服务器
	return c;
}

#define MAX_ACCEPTS_PER_CALL 1000
static void acceptCommonHandler(int fd, int flags, char *ip) {
	client *c;
	// 创建一个客户端
	if ((c = createClient(fd)) == NULL) {
		close(fd); /* May be already closed, just ignore errors */
		return;
	}
}

/*
 * 接收新请求
 */
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
	int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
	char cip[NET_IP_STR_LEN];
	UNUSED(el);
	UNUSED(mask);
	UNUSED(privdata);

	while(max--) {
		cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
		if (cfd == ANET_ERR) {
			return;
		}
		// 处理命令
		acceptCommonHandler(cfd,0,cip);
	}
}

int processInlineBuffer(client *c) {
	char *newline;
	int argc, j;
	sds *argv, aux;
	size_t querylen;

	/* 查找\n第一次出现的位置 */
	newline = strchr(c->querybuf,'\n');

	/* 如果没有\r\n，什么都不做 */
	if (newline == NULL) {
		return C_ERR;
	}

	/* 处理\r\n */
	if (newline && newline != c->querybuf && *(newline-1) == '\r')
		newline--;

	/* 使用\r\n分割请求内容 */
	querylen = newline-(c->querybuf);
	aux = sdsnewlen(c->querybuf,querylen);
	argv = sdssplitargs(aux,&argc);
	sdsfree(aux);
	if (argv == NULL) {
		return C_ERR;
	}

	/* 取第一行后的数据保存到buffer */
	sdsrange(c->querybuf,querylen+2,-1);

	/* 把参数数组添加到客户端结构体 */
	if (argc) {
		if (c->argv) zfree(c->argv);
		c->argv = zmalloc(sizeof(robj*)*argc);
	}

	/* 为所有参数创建redis对象 */
	for (c->argc = 0, j = 0; j < argc; j++) {
		if (sdslen(argv[j])) {
			c->argv[c->argc] = createObject(OBJ_STRING,argv[j]);
			c->argc++;
		} else {
			sdsfree(argv[j]);
		}
	}
	zfree(argv);
	return C_OK;
}

int processMultibulkBuffer(client *c) {
	char *newline = NULL;
	int pos = 0, ok;
	long long ll;

	if (c->multibulklen == 0) {
		/* Multi bulk length cannot be read without a \r\n */
		newline = strchr(c->querybuf,'\r');
		if (newline == NULL) {
			return C_ERR;
		}

		/* Buffer should also contain \n */
		if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
			return C_ERR;

		/* We know for sure there is a whole line since newline != NULL,
		 * so go ahead and find out the multi bulk length. */
		ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
		if (!ok || ll > 1024*1024) {
			return C_ERR;
		}

		pos = (newline-c->querybuf)+2;
		if (ll <= 0) {
			sdsrange(c->querybuf,pos,-1);
			return C_OK;
		}

		c->multibulklen = ll;

		/* Setup argv array on client structure */
		if (c->argv) zfree(c->argv);
		c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
	}

	while(c->multibulklen) {
		/* Read bulk length if unknown */
		if (c->bulklen == -1) {
			newline = strchr(c->querybuf+pos,'\r');
			if (newline == NULL) {
				if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
					return C_ERR;
				}
				break;
			}

			/* Buffer should also contain \n */
			if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
				break;

			if (c->querybuf[pos] != '$') {
				return C_ERR;
			}

			ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
			if (!ok || ll < 0 || ll > 512*1024*1024) {
				return C_ERR;
			}

			pos += newline-(c->querybuf+pos)+2;
			if (ll >= PROTO_MBULK_BIG_ARG) {
				size_t qblen;

				/* If we are going to read a large object from network
				 * try to make it likely that it will start at c->querybuf
				 * boundary so that we can optimize object creation
				 * avoiding a large copy of data. */
				sdsrange(c->querybuf,pos,-1);
				pos = 0;
				qblen = sdslen(c->querybuf);
				/* Hint the sds library about the amount of bytes this string is
				 * going to contain. */
				if (qblen < (size_t)ll+2)
					c->querybuf = sdsMakeRoomFor(c->querybuf,ll+2-qblen);
			}
			c->bulklen = ll;
		}

		/* Read bulk argument */
		if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
			/* Not enough data (+2 == trailing \r\n) */
			break;
		} else {
			/* Optimization: if the buffer contains JUST our bulk element
			 * instead of creating a new object by *copying* the sds we
			 * just use the current sds string. */
			if (pos == 0 &&
					c->bulklen >= PROTO_MBULK_BIG_ARG &&
					(signed) sdslen(c->querybuf) == c->bulklen+2)
			{
				c->argv[c->argc++] = createObject(OBJ_STRING,c->querybuf);
				sdsIncrLen(c->querybuf,-2); /* remove CRLF */
				/* Assume that if we saw a fat argument we'll see another one
				 * likely... */
				c->querybuf = sdsnewlen(NULL,c->bulklen+2);
				sdsclear(c->querybuf);
				pos = 0;
			} else {
				c->argv[c->argc++] =
					createStringObject(c->querybuf+pos,c->bulklen);
				pos += c->bulklen+2;
			}
			c->bulklen = -1;
			c->multibulklen--;
		}
	}

	/* Trim to pos */
	if (pos) sdsrange(c->querybuf,pos,-1);

	/* We're done when c->multibulk == 0 */
	if (c->multibulklen == 0) return C_OK;

	/* Still not ready to process the command */
	return C_ERR;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
}

void processInputBuffer(client *c) {
	/* 如果querybuf不为空，一直处理 */
	while(sdslen(c->querybuf)) {
		/* 设置请求类型：批量/单个 */
		if (!c->reqtype) {
			if (c->querybuf[0] == '*') {
				c->reqtype = PROTO_REQ_MULTIBULK;
			} else {
				c->reqtype = PROTO_REQ_INLINE;
			}
		}

		// 解析参数
		if (c->reqtype == PROTO_REQ_INLINE) {
			if (processInlineBuffer(c) != C_OK) break;
		} else if (c->reqtype == PROTO_REQ_MULTIBULK) {
			if (processMultibulkBuffer(c) != C_OK) break;
		}
		printf("argc : %d\n", c->argc);
		printf("argv: %s\n", (char *)c->argv[0]->ptr);
		/* Multibulk processing could see a <= 0 length. */
		if (c->argc == 0) {
			resetClient(c);
		} else {
			/* 处理命令，仅当命令被成功执行后才重置客户端 */
			if (processCommand(c) == C_OK) {
				resetClient(c);
			}
		}
	}
}

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
	client *c = (client*) privdata;
	int nread, readlen;
	size_t qblen;

	readlen = PROTO_IOBUF_LEN;


	qblen = sdslen(c->querybuf);
	if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
	c->querybuf = sdsMakeRoomFor(c->querybuf, readlen); // 创建SDS字符串保存客户端的请求
	nread = read(fd, c->querybuf+qblen, readlen); // 读取请求内容
	if (nread == -1) {
		if (errno == EAGAIN) {
			return;
		} else {
			return;
		}
	} else if (nread == 0) {
		return;
	}

	sdsIncrLen(c->querybuf,nread);
	/*
	 * 处理请求
	 */
	processInputBuffer(c);
}

